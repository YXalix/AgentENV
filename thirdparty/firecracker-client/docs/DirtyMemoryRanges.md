# DirtyMemoryRanges

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**page_size** | **i32** | Page size used to interpret dirty bits. | 
**memory_size** | **i64** | Total byte size of the contiguous memory snapshot image. | 
**ranges** | [**Vec<models::DirtyMemoryRange>**](DirtyMemoryRange.md) |  | 
**dirty_ranges** | [**Vec<models::DirtyMemoryRange>**](DirtyMemoryRange.md) | Guest pages written during the current dirty-log window, coalesced. Cleared by this query, so query it at most once per window. | 
**discarded_ranges** | [**Vec<models::DirtyMemoryRange>**](DirtyMemoryRange.md) | Guest pages discarded via balloon free-page reporting since the previous query, coalesced. Their content reads as zero afterwards. Cleared by this query. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


