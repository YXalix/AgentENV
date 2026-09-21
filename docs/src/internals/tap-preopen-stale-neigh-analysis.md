# preopen_tap 池化 slot 的跨租户 STALE 邻居表项：envd 就绪尾延迟分析与修复计划

> 问题分析与修复计划文档，配套（而非替代）
> [TAP fd Slot Ownership](./tap-fd-slot-ownership-design.md)（下称"v3 设计文档"）。
>
> - AgentENV：`feat-tap-v3`；Firecracker：`feat-tap-v2` @ `440a4e5c8`
> - 内核实证环境：openEuler 6.6.0-145 aarch64（2026-09，本机 root）
> - 关联前科：issue #272（ARP retrans 导致 resume 尾延迟，
>   `src/sandbox/network/slot.rs:231`、`slot.rs:311`）
>
> 状态：内核机制已完整实证（§4，五组实验）；修复方案已定案并实现
> （§6：双端常量 MAC，交接时 flush 经评估后撤下，§6.2）；线上判别
> 命令见 §7.2，作为修复 PR 的前置/伴随证据执行。

## 1. 现象

`firecracker.preopen_tap = true` 时，沙箱启动的 envd 就绪等待
（`wait_for_ready`，`src/sandbox/envd.rs:109`）稳定在 **~10000 ms**；
置 `false` 则为 **~100 ms**，相差两个数量级。

测量背景与参数（`config/default.toml:196-198`、`envd.rs:23`）：

- 场景为快照恢复/模板启动（guest 已带完整协议栈，envd 已在内存中监听，
  健康路径基线即 ~100 ms）；
- 探测循环：单次 HTTP 探测超时 1 s（`HEALTH_PROBE_TIMEOUT`），探测间隔
  3 ms（`envd.poll_ms`），总超时 60 s（`envd.init_timeout_secs`）。

~10 s ≈ 连续 ~10 次探测超时后**自愈**——不是"envd 起得慢"，而是探测包
在某个层面被黑洞，直到内核某个 ~5 s 级定时器走完。这个"5 s 阶梯 + 自愈"
的指纹直接指向 IPv4 邻居（ARP）状态机。

## 2. 结论（TL;DR）

**根因**：netns 内 tap0 上对 `vm_ip`（默认规划恒为 `169.254.0.21`，
`address_plan.rs:44-50`）的邻居表项在 slot 池化期间残留**上一任租户的
guest MAC**。`preopen_tap` 让 TAP 队列永不 detach，tap0 carrier 恒为
UP，该表项跨租户存活；`drain_tap_queue` 只排空 L3 队列里的积压帧，
**不清理 L2 邻居表**。新租户恢复后的第一批探测 SYN 被内核按 STALE
表项封装成发往旧 MAC 的帧，guest 网卡静默丢弃；直到邻居状态机走完
`STALE → DELAY（5 s）→ PROBE（3 次单播探测失败）→ 回收 → 广播 ARP`
才恢复，单轮 5.3–8.2 s，叠加杂散流量触碰表项可到 ~10 s。

**为什么关掉 preopen 就没事**（已实证）：FC 进程退出 → 最后一个队列
detach → tap0 carrier off，**内核在 carrier 丢失时清空该设备的 IPv4
邻居表**。下次 attach 后是空表，首个探测包直接触发广播 ARP，毫秒级
解析。v3 把 detach 消掉的同时，把这个一直在"免费"发生的邻居表清理
也消掉了——这是 v3 设计未记录的隐式依赖。

**修复方向**（已实现）：把跨租户会变化的两个 MAC 都钉成节点级常量
——fresh 启动固定 guest MAC、slot 创建时固定 tap0 MAC。模板重建后，
恢复路径的 guest MAC 同样恒定；netns 与 guest 两张邻居表的表项由此
跨租户恒对，问题在源头消失（§6.1）。前提是"重制全部快照 + 清理升级
窗口的存量 Paused 沙箱/用户快照"这一运营承诺（§6.2/§8）；交接时的
条件 flush 方案已评估、实现后撤下（决策记录见 §6.2，形态存档于
§6.5）。

## 3. 探测路径与故障点

### 3.1 帧路径

```text
host (envd 探测 TCP SYN)
  → 路由 host_interaction_ip/32（slot 创建时加，slot.rs:849-865）
  → veth-<idx> → netns vpeer
  → DNAT host_interaction_ip → vm_ip（slot.rs:733-737）
  → 路由命中 tap0 的直连网段（tap_ip/30）
  →【故障点】L2 邻居解析：netns 需要在 tap0 上把 vm_ip 解析成 guest MAC
  → 队列（tun socket）→ FC 读 fd 3 → 注入 guest eth0
```

host→veth 一跳的邻居（veth_vm_ip）两侧 MAC 都随 slot 固定，不受影响；
唯一的跨租户变量是 **tap0 上的 `vm_ip → guest MAC` 表项**。

### 3.2 guest MAC 为什么会跨租户变化

- 全新启动：`add_network_interface(..., guest_mac = None, ...)`
  （`sandbox.rs:2307`）→ Firecracker **每 VM 随机生成**；
- 快照恢复：MAC 随快照的 vm_state 而来，恢复 API 的
  `network_overrides` 只覆盖 `host_dev_name`（`fdp:3:tap0`，
  `sandbox.rs:2088-2091`、`instance.rs:73-74`），**不能覆盖 MAC**。

slot 池复用（`WarmPool<Slot>`）保证同一 `vm_ip` 先后服务不同 MAC 的
guest。对称地，guest 内存里 `tap_ip → 本 slot tap0 MAC` 的表项在跨
slot 恢复时同样是错的（tap0 MAC 由 `ip tuntap add` 在 slot 创建时随机
分配、slot 生命周期内稳定，slot.rs:556-567）。当前故障模式下这两个
方向的错误由同一轮广播 ARP 一并终结（§4 场景 1 的恢复路径）；修复
方案则让两张表恒对（§6.1）。

### 3.3 preopen 前后 tap0 的生命周期差异（不对称的来源）

| | `preopen_tap = false` | `preopen_tap = true` |
| --- | --- | --- |
| 队列持有者 | FC 进程（spawn 时按名 attach） | slot（`attach_tap_queue`，slot.rs:344） |
| pause/stop 时 | FC 退出 → detach → **carrier off** | 只关 dup，队列仍 attach |
| 池化期间 carrier | OFF（入向帧在内核早期丢弃） | **恒 UP**（`/32` 路由 + DNAT 仍活，杂散帧入队） |
| 池化期间邻居表 | detach 即被内核清空（§4 场景 5 实证） | **旧表项存活**（仅 gc 定时器 ~60 s 后回收，杂散流量触碰反而续命/扰动） |
| 新租户首包 | 空表 → 广播 ARP → 毫秒级 | STALE(旧 MAC) → 首批帧发往旧 MAC → 黑洞 5–10 s |

## 4. 内核机制实证（本机，无需 VM）

全部可复现（脚本见附录）。相关内核参数（root ns 默认值）：
`delay_first_probe_time=5`、`ucast_solicit=3`、`mcast_solicit=3`、
`retrans_time_ms=1000`。注意沙箱 netns 内只调过 tap0/vpeer 的
`retrans_time_ms=100`（issue #272），**没有也不需要调
`delay_first_probe_time`——本次故障的 5 s 恰好卡在这个没调的旋钮上**。

### 场景 1（复现）：veth 对端静默换 MAC + 表项压 STALE

| 相对时刻 | 邻居状态 | 说明 |
| --- | --- | --- |
| 0.00 s | STALE（lladdr=旧 MAC） | 起 ping，首个包按旧 MAC 发出 |
| 0.21 s | DELAY | 对端内核静默丢弃（dst MAC 不匹配） |
| 5.14 s | PROBE | `delay_first_probe_time = 5s` 整 |
| 8.33 s | REACHABLE（lladdr=新 MAC） | 3×1 s 单播探测失败后广播 ARP，立即解析 |

首个 ICMP 应答 **8.16 s**，窗口内丢包 53.6%。ping 每 200 ms 重发无济于
事——与 envd 3 ms 重试依然等 10 s 的现象同构：**恢复由邻居状态机定时器
触发，与流量重试频率无关**。

### 场景 2（对照 A）：STALE 但 MAC 一致

（对应"同一快照反复恢复到同一 slot"）首个应答 **0.001 s**。STALE 表项
本身无害——内核允许使用 STALE 表项发送，出问题的是**内容过期**。

### 场景 3（对照 B）：换 MAC + 删除表项

（对应 §6.4 备选方案"交接时清表"的机制验证，以及 fresh slot 的天然
行为）首个应答 **0.002 s**：无表项 → 首包直接广播 ARP → 毫秒级解析。

### 场景 4（对照 C）：veth carrier 周期

STALE + 换 MAC 后将链路 carrier off 再 on（对端 admin down/up）：carrier
掉时表项**即被清空**，恢复后首个应答 **0.00 s**。

### 场景 5（真实 tun 设备）：队列 attach/detach 即 carrier 开关，detach 清表

在独立 netns 中 `ip tuntap add tap0` + 注入一条 STALE 表项后：

```text
无队列            tap0 DOWN  <NO-CARRIER,...,UP>          （无表项操作）
attach 队列       tap0 UP    <BROADCAST,MULTICAST,UP,LOWER_UP>
  └─ 表项在场：10.98.0.2 lladdr 02:11:22:33:44:55 STALE
detach 队列       tap0 DOWN  <NO-CARRIER,...,UP>
  └─ 表项消失（carrier 丢失触发该设备 IPv4 邻居表清理）
```

这就是 `preopen_tap=false` 一直免费享受、而 v3 意外移除的隐式清理。
（行为依赖内核对 carrier 丢失的邻居清理，本机 6.6 实证；§6.1 的修复
不依赖该行为，任何内核下都成立。）

### 时间账

沙箱 netns 内 tap0 的 `retrans_time_ms=100`，单轮状态机 =
5 s（DELAY）+ ~0.3 s（3×100 ms 单播探测）+ 毫秒级（广播 ARP）≈
**5.3–5.6 s**。观测 ~10 s 与"杂散流量在 DELAY 窗口内反复触碰表项使
状态机重来一轮"或"首轮广播落在 VM 恢复前"相符，量级与自愈特征一致。
精确轮次可在修复前用 §7.2 的 watch 命令现场观察。

## 5. 为什么现有防线没拦住

1. **`drain_tap_queue`（slot.rs:411）是 L3 的**：它排空 tun socket 里
   已入队的帧（v3 设计文档 §2"drain 是跨租户正确性问题"指的是这一层），
   但邻居表是内核里的 L2 状态，队列排空不影响表项内容。v3 识别了
   "idle 期间入站帧会缓存"的 L3 面，漏了同源的 L2 面。
2. **issue #272 的调优治的是另一条路径**：`retrans_time_ms=100` 加速
   "该发的 ARP 重发得快"；本次是"内核信任了一条不该信的 STALE 表项"，
   卡点在 `delay_first_probe_time=5s`，与 retrans 无关。
3. **carrier-off 清表是不曾被告知的依赖**：v3 设计文档的目标之一是
   "pause/stop 0 个 tun ioctl"，实现方式（队列永不 detach、carrier 恒
   UP）顺带关闭了每次租户更替时的邻居表隐式清理。设计阶段没有任何
   文档记录"detach 一直在帮我们清邻居表"这个副作用。

## 6. 修复计划：双端常量 MAC（Fix B，P0）

### 6.1 方案

问题涉及两张邻居表，各有一个跨租户变量（§3.2、§3.3）：

| 表 | 位置 | 表项 | 跨租户变量 |
| --- | --- | --- | --- |
| netns 侧 | netns 内核 | `vm_ip` → guest MAC | fresh 由 FC 随机生成；恢复随快照 |
| guest 侧 | guest 内存（随快照冻结） | `tap_ip` → tap0 MAC | tap0 MAC 由 `ip tuntap add` 在 slot 创建时随机分配，slot 间不同 |

方案是把两个变量都换成节点级常量：

- **B1（guest 端）**：fresh 启动固定 `guest_mac = C_GUEST`
  （`sandbox.rs:2307` 由 `None` 改常量）。模板构建即 fresh 启动，
  重建后的模板快照内嵌 `C_GUEST`，恢复路径同样恒为 `C_GUEST`。
- **B2（tap0 端）**：slot 创建时在 `ip tuntap add tap0` 之后追加
  `ip link set tap0 address C_TAP`（`slot.rs:556-567` 同一 setup
  线程、同一 `run_with_scoped_capabilities(CAP_NET_ADMIN)` 模式）。
  guest 快照里冻结的 `tap_ip → C_TAP` 表项在任何 slot 上都正确。

**两端都必须固定的原因**（只做 B1 的缺口）：跨 slot 恢复时 netns 侧
表项虽恒对、SYN 能立即送达 guest，但 guest 的应答要发往快照里冻结的
**旧 slot tap0 MAC**，被当前 slot 的 tap0 按 OTHERHOST 丢弃；要等
netns 侧表项走 STALE→DELAY（5 s）→PROBE，单播 ARP 请求到达 guest 时
才顺带把 guest 侧表项更正——残留 ~5–6 s 尾巴（旧快照的 guest 表项
还可能冻结在 REACHABLE，见 §9.2，尾巴更长）。B2 把 guest 侧也变成
恒对，两个方向都不再依赖任何 ARP 事件。

拓扑上不需要全局唯一：每个 slot 的 netns 是独立 L2 域，guest MAC 与
tap0 MAC 都不出 netns（host 侧看到的是 veth/vpeer MAC，vpeer MAC 随
netns 持久、从不跨租户变化）。这正是 CubeSandbox 采用节点级常量
`mvmMacAddr` 的前提（`tap_device.go:139`）；其额外的静态 ARP 手段
我们不可用，见 §6.2 禁令。

### 6.2 决策记录：不做交接时 flush（信承诺、要最简）

曾设计并实现过 activate 时的条件 flush（读 tap0 上 `vm_ip` 表项：
不存在则不动；lladdr == `C_GUEST` 则降级 STALE——STALE 表项照常
发送，内容正确时零 ARP 零延迟，错误时走标准阶梯自愈；否则删除，
首包广播毫秒级重解析），实现并验证后撤下。理由：

- **稳态不需要它**：全部快照重制后，两张邻居表对所有租户恒对，表项
  由每个 slot 第一个租户的广播建立后始终正确——保留/降级/删除三个
  分支在稳态的数据路径上完全等价；
- **它只为混布期与漏设路径兜底**（升级窗口的存量 Paused 沙箱/用户
  快照、未来忘记传常量的启动分支），代价是 activate 上 ~90 行
  netlink 代码与每次交接一次 setns；
- 决策：以"重制全部快照 + 清理存量"的运营承诺换取最简实现。承诺
  破缺的后果是显式接受的：回到本 bug 量级（5–10 s；guest 侧冻结
  REACHABLE 的旧快照上界 ≈ 30 s + 8 s，§9.2），而非更糟。

若未来承诺无法维持，按 §6.5 存档的形态恢复。过渡期结束前禁止在
tap0 上配置 `nud permanent` 表项：permanent 不老化，对带任意 MAC 的
旧快照是**永久**黑洞（连 8 s 自愈都没有）；CubeSandbox 的静态 ARP
之所以可用，是其 MAC 由构造保证一致，混布期我们做不到。

### 6.3 实施要点

1. 两个常量集中定义一处（`address_plan.rs`，与 `vm_ip`/`tap_ip`
   同源），取本地管理位单播 MAC（`02:…`）；fresh 的 `guest_mac` 与
   slot 的 tap0 设置共用同一处定义，避免漂移（对齐 v3 "单一判断点"
   的 I2 习惯）。
2. B2 落在 `setup_namespace_internal` 的 tap0 创建处；B1 落在
   fresh 启动的 `add_network_interface`。均无 FC 侧改动。
3. 生效节奏：发布 AgentENV（fresh 立即生效）→ 重建模板（恢复路径
   生效）→ 旧快照自然淘汰。发布前抓的快照在过渡期回到本 bug 量级
   （5–10 s；冻结 REACHABLE 的上界 ≈ 30 s + 8 s，§9.2）。
4. 不加配置开关：常量即契约；若确需可配，必须强调全节点一致。

### 6.4 残留风险与缓解

| 风险 | 说明 | 缓解 |
| --- | --- | --- |
| 过渡期尾巴 | 升级窗口的存量 Paused 沙箱/用户快照带随机 MAC，恢复后回到本 bug 量级（5–10 s；冻结 REACHABLE 上界 ≈ 30 s + 8 s） | 模板重建排期 + 升级时清理存量；用 §7.3 的观测量化真实尾巴 |
| 漏设路径复发 | 任何未来 fresh 路径忘记传 `guest_mac` 即重新引入随机 MAC，无兜底 | 常量单点定义 + 单测锁定调用点；线上尾延迟回归可检出 |

### 6.5 备选方案记录

- **交接时条件 flush（已实现后撤下，§6.2）**：读 tap0 上 `vm_ip`
  表项，lladdr == `C_GUEST` 则降级 STALE（STALE 表项照常发送：正确
  时零 ARP 零延迟，错误时 DELAY 5 s → PROBE → 广播，~5.3–8.2 s
  自愈），否则删除。注意"降级 STALE 而非保留 REACHABLE"是必要的：
  保留在混布期对旧快照是 ~35 s 黑洞（REACHABLE 错误表项在 guest 空闲
  时无任何重解析机会，等 30 s 自然降级后再走阶梯）。比无条件删表好
  在稳态零 ARP；撤下仅因稳态不需要它。恢复形态：setns 线程 +
  `run_with_scoped_capabilities(CAP_NET_ADMIN)` + rtnetlink
  GETNEIGH/NEWNEIGH/DELNEIGH，落在 `NetworkManager::activate()`。
- **无条件删表（原 Fix A）**：自愈型，覆盖一切路径，稳态每次交接
  一轮广播；常量方案落地后无单独价值。
- **快照记录 `guest_mac` 字段 + replace**：YAGNI（flush 撤下后无
  消费方；当前快照记录亦无 schema 版本，仅 `runtime_versions` +
  serde default 惯例，`snapshot/types/snapshot.rs:322`）。
- **池化期间阻断入向（撤回 `/32` 路由 / nft / eBPF default-deny）**：
  每生命周期在 resume 路径引入 2 次主机侧 rtnl 操作，违背 v3
  "resume 路径 0 rtnl"；否决。若未来杂散流量成为 drain 的真实负载
  再议（CubeSandbox 用 eBPF default-deny 实现同效，
  `tap_lifecycle.go:327`）。

### 6.6 Fix D（P1，顺带）：offload 预设是死代码，账单不成立

v3 设计文档 §4.1 写 `FC_TAP_OFFLOAD_PRESET = CSUM|TSO4|TSO6 = 0x07`，
但两个仓库的实现都是**含 `TUN_F_UFO`（0x10）的 0x17**
（AgentENV `slot.rs:74`；FC `device.rs:773`）。FC 侧 skip 条件要求
`build_tap_offload_features(acked) == 0x17`（`device.rs:956`），而
现代 Linux guest 不再 ack `VIRTIO_NET_F_GUEST_UFO`，协商值恒为 0x07
——**skip 永不触发，activate 仍每次下发 `TUNSETOFFLOAD(0x07)` 覆盖
预设**。正确性无恙（这也是本次 10 s 问题与 offload 无关的原因），但
v3 设计文档 §7.2 "TUNSETOFFLOAD 计数 = 0" 的账单目前不成立。

处理：两边常量同步去掉 UFO 位改为 0x07（新内核 tun 对 UFO 已不敏感，
等价），skip 即真正生效，热路径回到 0 次 tun ioctl；需与 FC 侧
`LAUNCHER_PRESET_OFFLOAD` 配对发布（同 v3 的配对纪律，单边修改不死：
值不等时 FC 会下发协商值纠正）。或维持现状并勘误 v3 文档。

### 6.7 不做什么

- 不改 `drain_tap_queue` 语义（L3 排水正确且必要）；
- 不调 `delay_first_probe_time` sysctl——治标且引入新全局副作用；
- 不在 tap0 上配置 `nud permanent` 表项（§6.2）；
- 不给快照记录加 guest_mac 字段（§6.5）；
- 不动 Firecracker（修复全在 AgentENV 侧；仅 Fix D 需常量配对）。

## 7. 验证与验收

### 7.1 已完成

- 内核机制五组实验（§4，本机 6.6，数据即上文；脚本见附录）；
- 代码事实核对（§3 全部 file:line 均为当前 `feat-tap-v3` 实测位置）。

### 7.2 修复前线上判别（修复 PR 的前置证据，各一条命令）

```bash
# 方法 1（只读，5 s）：闲置池的 slot 是否残留 STALE 表项
for f in $AENV_RUNTIME_PATH/netns/*; do
  echo "== $f"; nsenter --net=$f ip neigh show dev tap0 2>/dev/null
done
# 预期：preopen=on 时闲置 slot 有一条 169.254.0.21 ... STALE（旧租户 MAC）

# 方法 2（定案）：启动卡住的 ~10 s 窗口内删表项，观察 wait_for_ready 立即成功
N=$(ls -t $AENV_RUNTIME_PATH/netns | head -1)
watch -n0.3 "nsenter --net=$AENV_RUNTIME_PATH/netns/$N ip neigh show dev tap0"
nsenter --net=$AENV_RUNTIME_PATH/netns/$N ip neigh del 169.254.0.21 dev tap0
```

注：netns 文件不在 `/var/run/netns`，用 `nsenter --net=<路径>`。
对照观察 STALE 期 MAC 与最终 REACHABLE 期 MAC 是否不同（跨租户
churn 的直接证据）。

### 7.3 修复后验收

1. **行为级（核心指标）**：租户矩阵循环（间隔 <60 s，避开 gc 定时器
   干扰），`wait_for_ready` p50/p95 全部回到 ~100 ms 量级：
   fresh→fresh、重建模板 A→重建模板 B（跨 slot）、同模板循环、
   preopen 开/关。
2. **常量落位断言**：`nsenter --net=<netns> ip link show tap0` 的
   MAC == `C_TAP`；fresh 启动后 guest `eth0` == `C_GUEST`；重建模板
   恢复后 netns neigh 表项 lladdr == `C_GUEST`。
3. **过渡期观测**：用改动前抓的旧快照启动一次，记录其尾延迟（预期
   回到本 bug 量级），作为模板重建排期与存量清理紧迫度的输入。
4. **单测**：`test_network_lifecycle`（slot.rs:1350）增加 tap0 MAC ==
   `C_TAP` 断言；`build_ip_boot_arg` 等不受影响。
5. **不回归 v3 账单**：`ip link set address` 只在 slot 创建冷路径
   执行，tap-rtnl.bt 确认 resume 路径 tun ioctl 计数仍为 0；drain
   行为不变。
6. **preopen=off 回归**：全流程行为与上游一致。

### 7.4 量化报告

修前/修后各跑一轮 §7.3.1 循环，报告 `wait_for_ready` 分布
（p50/p95/max）对比表，归档到本文档。

## 8. 兼容性与回滚

- **双端常量（B1+B2）**：只影响新启动 VM 与新建 slot 的 MAC 分配，
  不改任何持久化格式（快照内 iface 配置原样存取，只是内容变成
  常量 MAC）；旧快照/旧模板不受影响，按 §6.3 的节奏自然淘汰。
  回滚 = revert：注意回滚会制造新一轮新旧 MAC 混合，重现过渡期
  尾巴，建议低峰操作。
- guest 镜像内如有按 MAC 匹配的配置（udev `.link`、容器 MAC 等少见
  情况），常量化后以新 MAC 生效，发布前抽查模板。
- **Fix D**：常量改动需 AgentENV/FC 配对发布；不配对也不破坏正确性
  （FC 会在值不等时下发协商值），只影响 skip 是否生效。

## 9. 开放问题

1. **观测 ~10 s vs 单轮理论 5.3–5.6 s**：怀疑杂散流量在 DELAY 窗口
   反复触碰表项导致状态机重跑，或首轮广播 ARP 落在 VM resume 前被
   drain/未读丢弃。修复前用 §7.2 的 watch 现场确认轮次即可，不影响
   结论与修复方案。
2. **过渡期尾巴上界**：旧快照（双端常量化之前抓的）的 guest 侧表项
   随快照冻结，恢复瞬间可能仍处 REACHABLE（guest 时钟不感知池化
   时长），其尾巴理论上界 ≈ base_reachable_time（30 s）+ DELAY/PROBE
   （~8 s）。新快照（B1+B2 之后）两表恒对，与 ARP 事件无关。过渡期
   实测尾巴（§7.3）决定模板重建排期与存量清理的紧迫度；若不可接受，
   按 §6.5 恢复条件 flush 兜底。
3. **同快照循环（MAC 不变）场景下 preopen=on 理论应为 ~100 ms**
   （场景 2）。若线上此场景也慢，则另有原因，需另行排查（本分析不
   覆盖）。

## 附录：复现脚本

与 §4 数据对应，root 执行，自清理。

```bash
#!/usr/bin/env bash
# 场景 1–4：veth 对端换 MAC 模拟"新租户"，本端观察恢复时延
set -e
NS=aenv-demo-g
ip netns add $NS
ip link add v0 type veth peer name v1
ip link set v1 netns $NS
ip addr add 10.99.0.1/30 dev v0 && ip link set v0 up
ip netns exec $NS ip addr add 10.99.0.2/30 dev v1 && ip netns exec $NS ip link set v1 up
sleep 0.3
ping -c1 -W1 10.99.0.2 >/dev/null          # 学到对端 MAC
for k in delay_first_probe_time ucast_solicit retrans_time_ms; do
  echo "$k=$(cat /proc/sys/net/ipv4/neigh/v0/$k)"
done
first_reply() {  # $1=ping 日志
  grep -m1 -o '^\[[0-9.]*\]' "$1" | tr -d '[]'
}

# 场景 1：换 MAC + STALE（preopen=on 池化残留的复现）
OLD=$(ip netns exec $NS ip link show v1 | awk '/link\/ether/{print $2}')
ip netns exec $NS ip link set dev v1 address 02:00:00:00:00:99
ip neigh change 10.99.0.2 lladdr $OLD dev v0 nud stale
START=$(date +%s.%N)
timeout 16 ping -D -i 0.2 -w 15 10.99.0.2 >/tmp/ping1.log 2>&1 || true
awk -v s=$START -v f=$(first_reply /tmp/ping1.log) \
  'BEGIN{printf "S1 换MAC+STALE: 首应答 %.2f s\n", f-s}'
# 同步观察: watch -n0.3 ip neigh show dev v0
#   STALE → DELAY → (5s) → PROBE → (3x1s) → REACHABLE(新MAC)

# 场景 2：STALE 但 MAC 一致（同快照循环）
ip neigh change 10.99.0.2 dev v0 nud stale
START=$(date +%s.%N)
timeout 8 ping -D -i 0.2 -w 7 10.99.0.2 >/tmp/ping2.log 2>&1 || true
awk -v s=$START -v f=$(first_reply /tmp/ping2.log) \
  'BEGIN{printf "S2 STALE+MAC一致: 首应答 %.3f s\n", f-s}'

# 场景 3：换 MAC + 删表项（备选方案机制验证 / fresh slot 行为）
ip netns exec $NS ip link set dev v1 address 02:00:00:00:00:aa
ip neigh del 10.99.0.2 dev v0
START=$(date +%s.%N)
timeout 8 ping -D -i 0.2 -w 7 10.99.0.2 >/tmp/ping3.log 2>&1 || true
awk -v s=$START -v f=$(first_reply /tmp/ping3.log) \
  'BEGIN{printf "S3 换MAC+删表项: 首应答 %.3f s\n", f-s}'

# 场景 4：carrier 周期（preopen=off 的隐式清理）
ip neigh replace 10.99.0.2 lladdr 02:00:00:00:00:aa dev v0 nud stale
ip netns exec $NS ip link set v1 down          # carrier off → 表项被清
sleep 0.5; ip neigh show dev v0                # 空
ip netns exec $NS ip link set dev v1 address 02:00:00:00:00:bb
ip netns exec $NS ip link set v1 up
START=$(date +%s.%N)
timeout 8 ping -D -i 0.2 -w 7 10.99.0.2 >/tmp/ping4.log 2>&1 || true
awk -v s=$START -v f=$(first_reply /tmp/ping4.log) \
  'BEGIN{printf "S4 换MAC+carrier周期: 首应答 %.2f s\n", f-s}'

ip netns del $NS; ip link del v0 2>/dev/null || true

# 场景 5：真实 tun 设备，队列 attach/detach 控制 carrier，detach 清表
ip netns add aenv-tun-test
ip netns exec aenv-tun-test ip tuntap add tap0 mode tap
ip netns exec aenv-tun-test ip addr add 10.98.0.1/30 dev tap0
ip netns exec aenv-tun-test ip link set tap0 up
ip netns exec aenv-tun-test python3 -c '
import fcntl, struct, os, time
fd = os.open("/dev/net/tun", os.O_RDWR)
ifr = struct.pack("16sH22s", b"tap0", 0x0002 | 0x1000, b"")  # IFF_TAP|IFF_NO_PI
fcntl.ioctl(fd, 0x400454CA, ifr)                              # TUNSETIFF
time.sleep(60)
' &
HOLDER=$!
sleep 1
ip netns exec aenv-tun-test ip neigh replace 10.98.0.2 \
  lladdr 02:11:22:33:44:55 dev tap0 nud stale
echo "attach 期间:"; ip netns exec aenv-tun-test ip neigh show dev tap0
kill $HOLDER; wait $HOLDER 2>/dev/null || true; sleep 1
echo "detach 之后:"; ip netns exec aenv-tun-test ip neigh show dev tap0
# 空输出 = carrier off 清掉了表项
ip netns del aenv-tun-test
```

本机实测（2026-09，openEuler 6.6.0-145 aarch64）：

| 场景 | 首个应答 | 备注 |
| --- | --- | --- |
| S1 换 MAC + STALE | 8.16 s | 丢包 53.6%；状态机 5 s + 3×1 s 阶梯逐项吻合 |
| S2 STALE + MAC 一致 | 0.001 s | STALE 本身无害 |
| S3 换 MAC + 删表项 | 0.002 s | 删表即恢复（§6.4 备选方案机制） |
| S4 换 MAC + carrier 周期 | 0.00 s | carrier off 即清表 |
| S5 tun attach/detach | — | detach → carrier off → 表项消失 |
