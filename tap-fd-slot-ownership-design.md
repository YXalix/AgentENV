# TAP 队列 fd 所有权迁移：Slot 长持 fd（as-built）

> 面向评审与后续维护的实现文档，与代码同构。
>
> - AgentENV：`/root/github/AgentENV`，分支 `feat-tap`，提交 `e6afcf5`
> - Firecracker：`/root/github/firecracker`，分支 `feat-tap`，提交 `3266cc03c`
>
> 两个提交必须配对发布：AgentENV 产出 `fdp:` spec，Firecracker 消费之。
> 未包含该 Firecracker 提交的 bundle 上必须保持
> `firecracker.preopen_tap = false`。验证状态见 §7。

## 1. 背景与目标

### 1.1 问题

tun.c `__tun_chr_ioctl` 在函数入口 `rtnl_lock()`、出口 `rtnl_unlock()`，
**所有 ioctl 命令全程持有全局 rtnl 锁**。每发一个 tun ioctl 就付一次 rtnl
往返，高并发下所有沙箱的 tap 操作在此串行。

改动前的交接形态里，fd 在 spawn 的 pre-exec hook 中打开、随 FC 进程生死，
pause 又刻意杀进程释放内存，于是 fd 随进程死（`tun_detach`），下次 resume
重新 attach + 校验 + 配置。全 warm 命中时每个 pause→resume 周期：

| 阶段 | ioctl | 次数 |
| --- | --- | --- |
| resume（请求时刻） | TUNGETIFF + TUNSETVNETHDRSZ + TUNSETOFFLOAD | 3 |
| pause（FC 进程退出） | tun_chr_close → tun_detach | 1 |
| warm FC 池补充 | TUNSETIFF（spawn hook 打开） | 1 |
| **合计** | | **5** |

根因：队列 fd 的生命周期被绑死在 FC 进程上。**tun fd 只是打开文件描述，
谁持有引用队列就活着**——这是归属设计的结果，不是内核约束。

### 1.2 目标

fd 所有权从"每个 FC 进程"迁移到"每个 Slot"：

1. `TUNSETIFF` 与 `TUNSETVNETHDRSZ` 每 slot 一生一次（netns 创建时 attach
   并预设 vnet header）；
2. pause/FC 退出 **0 个** tun ioctl（只关 dup，Slot 持引用，detach 不触发）；
3. warm FC 池补充 **0 个** tun ioctl（spawn 只 `dup2`）；
4. 复用前排水（drain），杜绝跨租户读到上一个生命周期的帧。

账单：5 → **1**（每周期只剩 activate 时依赖 guest 协商的 `TUNSETOFFLOAD`，
这是用户侧下限）。

### 1.3 参照

CubeSandbox（`/root/github/CubeSandbox`）已验证同一模式：
`Cubelet/network/runtime/tap_lifecycle.go:45`（"A retained fd stays
attached … for its whole pool lifetime, so later handoffs only duplicate it
and never pay a TUNSETIFF on the hot path"）、`tap_device.go:211`
（创建时一次性配置）、`tap_lifecycle.go:97`（`drainTapFD`）。本方案把该
模式适配进 AgentENV 的 per-netns 隔离模型，不改隔离拓扑。

## 2. 关键事实（评审/修改前无需再查）

- **fd 语义**：`fork`/`dup2`/`F_DUPFD` 复制的是描述符；打开文件描述
  （`struct file` → `tun_file`）唯一。**最后一个**引用关闭才触发
  `tun_chr_close` → `tun_detach`（rtnl）。Slot 持引用时，FC 退出只减引用
  计数，detach 不发生。
- **O_CLOEXEC 只在 execve 时关闭**，fork 之后 exec 之前子进程里仍可用 →
  交接用 `dup2(slot_fd, 3)`；`dup2` 自动清除新 fd（fd 3）的 CLOEXEC，同号
  无操作时不清（file action 无 `F_SETFD`，spawn 侧对同号显式报错）。slot
  自己的 fd 必须 `O_CLOEXEC`（防止泄入 server spawn 的其他子进程 exec
  之后）。
- **fd 交接是 posix_spawn file action**（`ScopedSpawnSpec::fd_handoff`，
  `src/privileges.rs`）：`adddup2` 在 spawned 子进程内执行，天然子进程
  私有，父进程 fd 表零污染，跨线程并发 spawn 无竞争窗口。spec 只捕获裸
  fd 数值，调用方持 Slot 活过整个 spawn 即可（现状调用点天然满足）。
- **FC spawn 走 glibc `posix_spawn`**（`clone(CLONE_VM|CLONE_VFORK)` +
  execve）：server 进程不再 fork，零页表复制——带 `pre_exec` hook 的
  spawn 会失去该资格退化为真 fork（fat-fork，O(server RSS)），这就是本
  设计把交接改成 file action 的原因。`addchdir_np` 要求 glibc ≥ 2.29。
- **TUNSETIFF 必须在目标 netns 内执行**（按名查设备）→ 唯一合法位置是
  `Slot::create_network` 的 setup 线程（`unshare(CLONE_NEWNET)` 之后）。
  attach 完成后 fd 与执行线程所在 netns 无关，drain 可在任意线程读。
- **attach 不需要 CAP_NET_ADMIN**：`ip tuntap add` 建的持久设备 owner 为
  -1（任意用户可 attach）。setup 线程内本身也有 scoped caps 可用。
- **队列 fd 会 pin 住 netns**（`dev_net(tun->dev)` 引用）。Slot 的
  `OwnedFd` 字段在 `cleanup()` 之后随结构体自动 drop，引用归零后 netns
  才销毁——无需显式处理，字段注释已写明这个顺序依赖。
- **idle 期间入站帧会缓存进队列**：host 到 `host_interaction_ip/32` 的路由
  在 slot 回池后仍然存在（DNAT → tap0），杂散报文会排进 tap socket。
  → drain 是**跨租户正确性**问题，不只是卫生。
- **vnet header 契约**：Firecracker 的 `vnet_hdr_len()` =
  其 `virtio_net_hdr_v1` binding 的大小 = 12（含 `num_buffers`）。slot 侧
  `FC_VNET_HDR_LEN = 12` 与之配对；错配会**静默损坏帧**。
- Firecracker 侧（`3266cc03c`）：`Tap::from_preconfigured_fd` =
  `F_DUPFD_CLOEXEC`，**不做任何校验 ioctl**，`if_name` 取 spec 名（仍过
  长度校验）；`vnet_hdr_size_preset = true` 使 `Net::new` 跳过
  `TUNSETVNETHDRSZ`（该 ioctl 全树仅 `Net::new` 一个调用点）；
  `activate()` = `TUNSETOFFLOAD`（按 guest 协商）。

## 3. 设计

```
Slot::create_network（netns setup 线程内）
  ip tuntap add tap0 → IP/UP/路由/iptables（不变）
  → open(/dev/net/tun, O_RDWR|O_NONBLOCK|O_CLOEXEC)
  → TUNSETIFF(tap0, IFF_TAP|IFF_NO_PI|IFF_VNET_HDR)
  → TUNSETVNETHDRSZ(12)
  → OwnedFd 存入 Slot::tap_queue_fd        ← 每 slot 一次 attach + 预设

spawn FC：posix_spawn file action = dup2(slot_fd, 3)          ← 不再 open/ioctl
FC 消费：fdp: spec（信任预设，0 个校验/配置 ioctl，§5）

pause/stop：FC 进程退出 → 只关 dup → 引用计数>0 → 无 detach   ← teardown 0 ioctl
slot 回池：release() 时 drain → 池内保 fd
slot 再取：allocate_any / warm-FC acquire 时再 drain
slot 销毁：cleanup(veth/netns 文件) → 字段 drop 关 fd → netns 引用归零销毁
```

不变量（评审与测试围绕这些展开）：

- **I1**：`preopen_tap=true` 时，`create_network` 成功 ⟺
  `tap_queue_fd == Some`；attach 失败则整个创建失败（走现有回滚）。
- **I2**：spawn 用 fd 交接 ⟺ `host_dev_name` 用 `fdp:` spec——两者同源于
  `Slot::tap_handoff()`（spawn 侧与 `sandbox_host_dev_name()` 都是它的
  消费端），不允许各写各的判断。
- **I3**：所有释放路径（stop / Drop / warm 池清理）都经
  `NetworkManager::release`；所有取用路径（slot 池 acquire、warm-FC
  acquire）都 drain。
- **I4**：slot fd 一律 `O_CLOEXEC`；hook 捕获裸 fd 数值，调用方保证 Slot
  活过 spawn。

## 4. AgentENV 实现（`e6afcf5`）

### 4.1 `src/sandbox/network/slot.rs`

1. 私有字段 `tap_queue_fd: Option<OwnedFd>`，`Slot::new` 置 `None`。
   字段注释说明：fd pin 住 netns，`cleanup()` 后随字段 drop 释放。
2. 常量：`TUNSETIFF`、`TUNSETVNETHDRSZ`（`_IOW('T', 216, int)`，内核经
   userspace 指针读长度，故传引用）、`IFF_TAP|IFF_NO_PI|IFF_VNET_HDR`、
   `FC_VNET_HDR_LEN = 12`（与 Firecracker `vnet_hdr_len()` 配对）。
3. `setup_namespace_internal` 尾部（仍在 netns 线程内，iptables 之后）
   调 `attach_tap_queue`，仅当 `ConfigManager::global_config()
   .firecracker.preopen_tap`；结果经线程 join 存入 `self.tap_queue_fd`。
   attach 失败 → `create_network` 返回 Err → 现有回滚（bit 清除 +
   cleanup_armed）。
4. `attach_tap_queue(tap_name)`：`O_RDWR|O_NONBLOCK|O_CLOEXEC` 打开
   `/dev/net/tun`，`TUNSETIFF` 挂队列后**无条件** `TUNSETVNETHDRSZ(12)`
   ——预设是 `fdp:` 契约的一半，不是可选行为。
5. `Slot::tap_handoff()`（I2 的唯一判断点）：`tap_queue_fd` 存在时返回
   `TapHandoff { queue_fd }`。
6. `Slot::drain_tap_queue()`：非阻塞排空（对齐 CubeSandbox
   `drainTapFD`）：`read` 至 EAGAIN，64 KiB 缓冲、1024 次上限；best-effort，
   非 EAGAIN 错误记 debug 日志。

### 4.2 `src/sandbox/firecracker/instance.rs`

1. `TapHandoff { queue_fd: RawFd }`；`host_dev_name()` 固定产出
   `"fdp:3:tap0"`（`NET_TAP_FD = 3`，标准流后第一个槽位；子进程只消费
   被告知的描述符，覆盖继承位安全）。
2. spawn 组装 `ScopedSpawnSpec::fd_handoff = (queue_fd, NET_TAP_FD)`：
   launcher 线程（`aenv-process-launcher`）setns 进 netns、按 spec 清空
   capability 后直接 `posix_spawn`，`adddup2(queue_fd, 3)` 在子进程内
   把队列 dup 到 fd 3 并清除副本 CLOEXEC——不在父进程做任何 fd 复制，
   slot 自己的 fd 保留 CLOEXEC，不泄入无关子进程。`queue_fd == 3` 时
   显式报错（同号 dup2 是不清 CLOEXEC 的无操作；server 的 fd 3 被长持，
   实际不可达）。

### 4.3 `src/sandbox/network/manager.rs`

- `release()`：入池前 `slot.drain_tap_queue()`（I3）。
- `allocate_any()` 快路径：`try_acquire` 成功后 drain（覆盖 slot 池 idle
  窗口，也是 warm-FC 条目创建时的统一入口）。
- `cleanup_slot_and_release_bit*`（销毁路径）不 drain——netns 将销毁。

### 4.4 `src/sandbox/firecracker/sandbox.rs`、`pool.rs`、`mod.rs`

1. 冷启动（sandbox.rs:1808）、恢复（:2019）、warm 池预热（pool.rs:321）：
   spawn 时传对应 slot 的 `tap_handoff()`。
2. `sandbox_host_dev_name()`（sandbox.rs:1431，I2 消费端）：
   `tap_handoff()` 有则取 `host_dev_name()`，否则回退
   `SANDBOX_TAP_IFACE_NAME`（按名打开）。
3. `start_resume` 的 warm-FC 获取分支（sandbox.rs:1889）单独 drain：warm
   条目在 FC 池里 idle 时 FC 进程持有 fd 3 但从不读，帧积在共享 socket
   里；该路径不经过 `NetworkManager::release`。
4. `mod.rs` re-export `TapHandoff`。

### 4.5 配置与文档

仅一个开关：`firecracker.preopen_tap`（默认 `true`）。`src/cfg.rs`、
`config/default.toml`、`docs/src/configuration/reference.md` 描述一致：
slot 在 netns 创建时 attach 队列并预设 vnet header，spawn 以 `fdp:` spec
交接；pause/FC 退出不触发 detach。

### 4.6 spawn 走 posix_spawn（fat-fork 消除，`src/privileges.rs`）

压测中 pause/resume 的 CPU 火焰图曾显示 ~60% 耗在 FC spawn 的
fork+execve 上：带 `pre_exec` hook 的 spawn 失去 std/tokio 的
posix_spawn 资格，退化为裸 `fork()`，其成本是 O(server 页表)。现为消
除该热点：

- `spawn_scoped(ScopedSpawnSpec) -> ScopedChild`：launcher 线程
  （线程名保持 `aenv-process-launcher`）setns → 按 spec 清 capability →
  `libc::posix_spawn`。child 侧全部用 file actions/attributes 表达
  （stdin=/dev/null、stdout/stderr 日志文件或 /dev/null、
  `addchdir_np`、`SETPGROUP(0)`、`SETSIGDEF(SIGPIPE)`），env 继承
  `environ`；`addchdir_np` 要求 glibc ≥ 2.29。
- `ScopedChild` 取代 FC 场景的 `tokio::process::Child`：`id/try_wait/
  wait/start_kill` + Drop=SIGKILL+有界回收（对齐原 `kill_on_drop`）；
  async `wait` 用"先注册 SIGCHLD 流、再轮询 waitpid(WNOHANG)"协议。
- 语义保持：错误同步上报（glibc 对 execve/file-action 失败在返回值
  报错）、错误消息含 `ExitStatus` 展示、oom_score_adj/socket 清理/
  SIGTERM→SIGKILL stop 时序全部不变。`fdp:` 契约与 Firecracker 侧
  零改动。

## 5. Firecracker 实现（`3266cc03c`）

`src/vmm/src/devices/virtio/net/tap.rs`：

- `TapOpenSpec` 两种形态：`Name(&str)` 与 `Fdp { fd, if_name }`。
  `parse_tap_open_spec`：`fdp:` 前缀走 `parse_fd_spec_body`（坏数字 /
  `fd <= 2` / 空名同判 `InvalidFdSpec`），其余一律按普通接口名。
- `Tap::from_preconfigured_fd(fd, if_name)`：`F_DUPFD_CLOEXEC` 后**不做
  任何校验 ioctl**（消费幂等：parked fd 保持有效，进程内可重复消费）；
  `if_name` 直接取 spec 名（仍过 `build_terminated_if_name` 长度校验）。
  flags 契约：launcher 必须用 `IFF_TAP|IFF_NO_PI|IFF_VNET_HDR` 挂队列并
  预设 vnet_hdr_sz=12；违反契约在首个 read/write 处响亮失败，而非静默
  错连。
- `Tap.vnet_hdr_size_preset`（仅 fdp 路径置位）：`Net::new`
  （device.rs）据此跳过 `TUNSETVNETHDRSZ`。
- `activate()` 的 `TUNSETOFFLOAD` **不变**（依赖 guest 协商，无法预设）。
- 测试：`test_parse_tap_open_spec`（含 `fd:` 前缀按普通名解析、后续在
  接口创建处响亮失败的语义）、`test_from_preconfigured_fd_trusts_spec`、
  `test_open_named_or_fd_marks_fdp_as_preset`。

## 6. 提交与配对发布

```text
AgentENV     e6afcf5  feat(sandbox): hand Firecracker a slot-owned,
                      pre-configured TAP queue
Firecracker  3266cc03c feat(net): accept pre-configured TAP queues via
                      fdp: host_dev_name
```

配对语义：

- 新 FC + `preopen_tap=true`：`fdp:` 交接，快路径；
- 新 FC + `preopen_tap=false`：按名打开，与上游行为一致；
- 旧 FC（无 fdp:）+ `preopen_tap=true`：整个 spec 被当作接口名，netns 内
  无 CAP_NET_ADMIN → 设备创建响亮失败（不会静默错连）；
- bundle（`config/deps_manifest.toml`）升级到含 `3266cc03c` 的构建之前，
  保持 `preopen_tap = false`。

## 7. 验证与验收

### 7.1 功能（需 /dev/kvm、tun、CAP_NET_ADMIN/CAP_SYS_ADMIN）

已执行：

- 单测：AgentENV `sandbox::network::slot` / `sandbox::firecracker::instance`
  全过；Firecracker `devices::virtio::net::tap` 的 spec/fdp 测试全过
  （`test_tap_name` 的空名字分配竞争与 device 测试的 IO-safety abort 为
  上游既有问题，与本次改动无关，已用未改动树对照确认）；
- posix_spawn 迁移（§4.6）：`privileges` 全部单测过（exit status、
  file-action fd 交接并验证 dup2 清 CLOEXEC、capability 清零、drop 杀
  并回收），`sandbox::firecracker` 77 项全过（含 /bin/true 冷启动、
  /bin/echo 早退诊断、warm/cold 日志捕获迁移），`cargo fmt --check`、
  `cargo clippy -p agentenv --all-targets --all-features -- -D warnings`
  全过。

待补（ignored / 需特权环境；本验证机无 CAP_NET_ADMIN/CAP_SYS_ADMIN 与
/dev/kvm，与 e6afcf5 时点相同）：

1. ignored 集成测试：
   `spawn_with_netns_hands_preopened_tap_descriptor_to_child`（断言子进程
   fd 3 → `/dev/net/tun`）；`test_network_lifecycle` 第 1b 步
   （`TUNGETVNETHDRSZ` 回读 == 12）；`privileges` 的 netns 进入与
   capability 委派两个 ignored 测试；
2. 冷启动沙箱：`ls -l /proc/<fc_pid>/fd/3` 为 `/dev/net/tun`；**slot 存续
   期间 server 进程也持有一个 `/dev/net/tun` fd**；guest 网络连通（MMDS
   可拉取）；
3. pause → resume：恢复后连通；pause 落盘期间 host 上向
   `host_interaction_ip` 打少量杂散包（如 curl 超时），resume 后 guest
   **不应**收到上一个生命周期的帧（drain 生效的行为级验证）；
4. warm FC 池路径：制造池 idle 窗口后 resume，重复 3；
5. `preopen_tap=false` 回归：按名打开，行为与上游一致。

### 7.2 量化（bpftrace，脚本见附录）

压测 100 次 warm resume+pause 周期，对比改动前后：

| 指标（100 周期） | 改动前 | 现在 |
| --- | --- | --- |
| TUNSETIFF | ~100（池补充） | **0**（每 slot 一次） |
| tun_chr_close | ~100 | **0**（slot 销毁时才关） |
| TUNGETIFF | 100 | **0** |
| TUNSETVNETHDRSZ | 100 | **0**（attach 时每 slot 一次） |
| TUNSETOFFLOAD | 100 | 100（用户侧下限） |
| rtnl wait 直方图 | 基线 | 对比 |

`tun_chr_close == 0` 同时验证 teardown 归零与"无 fd 泄漏"（进程退出后
server 仍持有，slot 销毁时才关）。

## 8. 兼容性与回滚

- 快照内容零影响（只存内核接口名，不含机制细节）；
- `preopen_tap=false`：Slot 不 attach，全程按名回退；
- 旧 Firecracker bundle：`preopen_tap=true` 会响亮失败（§6），先置 false；
- 无持久化状态迁移：fd 不跨进程持久，回滚 = revert `e6afcf5` 即可
  （Firecracker 侧独立二进制，不受影响）。

## 9. 否决方案记录

- **FC 侧对 fdp 队列做 TUNGETIFF 校验再信任**：校验本身就是本次要消掉的
  rtnl ioctl；契约 + 首次读写响亮失败已覆盖错连风险。
- **SCM_RIGHTS 经 API socket 传 fd**：micro_http 流式解析，辅助数据绑定
  recvmsg 分块，不可行。
- **照搬 CubeSandbox 无 netns + BPF 数据面**：隔离模型重写，收益与本目标
  不成正比；其"池长持 fd + dup 交接 + drain"的核心已吸收。
- **宿主内核 rtnl 补丁**（attach 去 rtnl、只读 ioctl 移出大锁、
  TUNSETOFFLOAD no-op 快路径）：真正的根治项，独立跟进，先用量化数据决策。
- **消费后关闭 fd 3"省一次 detach"**：`F_DUPFD` 共享打开文件描述，本来
  就只有一次 detach，无收益（讨论中已纠正，勿再提出）。

## 附录：tap-rtnl.bt（量化脚本）

```text
#!/usr/bin/env bpftrace
/* 每 10s 窗口：tun ioctl 按 cmd 计数/延迟/错误、open/close、rtnl 等待/持锁 */
BEGIN { printf("tracing; window=10s\n"); }

kprobe:__tun_chr_ioctl { @io_start[tid] = nsecs; @io_cmd[tid] = arg1; }
kretprobe:__tun_chr_ioctl /@io_start[tid]/ {
  $us = (nsecs - @io_start[tid]) / 1000; $nr = @io_cmd[tid] & 0xff;
  $name = "other";
  if ($nr == 202) { $name = "TUNSETIFF"; }   if ($nr == 208) { $name = "TUNSETOFFLOAD"; }
  if ($nr == 210) { $name = "TUNGETIFF"; }   if ($nr == 216) { $name = "TUNSETVNETHDRSZ"; }
  @io_cnt[$name, comm] = count(); @io_us[$name, comm] = hist($us);
  if (retval != 0) { @io_err[$name, retval] = count(); }
  delete(@io_start[tid]); delete(@io_cmd[tid]);
}

kprobe:tun_chr_open  { @tun_open[comm] = count(); }
kprobe:tun_chr_close { @close_start[tid] = nsecs; }
kretprobe:tun_chr_close /@close_start[tid]/ {
  @tun_close_cnt[comm] = count();
  @tun_close_us[comm] = hist((nsecs - @close_start[tid]) / 1000);
  delete(@close_start[tid]);
}

kprobe:rtnl_lock { @lock_wait_start[tid] = nsecs; }
kretprobe:rtnl_lock /@lock_wait_start[tid]/ {
  @rtnl_wait_us = hist((nsecs - @lock_wait_start[tid]) / 1000);
  @lock_held_start[tid] = nsecs;
  delete(@lock_wait_start[tid]);
}
kprobe:rtnl_unlock /@lock_held_start[tid]/ {
  @rtnl_hold_us = hist((nsecs - @lock_held_start[tid]) / 1000);
  delete(@lock_held_start[tid]);
}

kprobe:register_netdevice { @netdev_register = count(); }

interval:s:10 {
  printf("\n===== %s =====\n", strftime("%H:%M:%S", nsecs));
  print(@io_cnt); print(@io_us); print(@io_err);
  print(@tun_close_cnt); print(@tun_close_us);
  print(@rtnl_wait_us); print(@rtnl_hold_us); print(@netdev_register);
  clear(@io_us); clear(@tun_close_us);
  clear(@rtnl_wait_us); clear(@rtnl_hold_us);
}
```

若宿主机无 `__tun_chr_ioctl` 符号，探测点改 `tun_chr_ioctl`（cmd 同为
arg1）。运行：`sudo bpftrace tap-rtnl.bt`。
