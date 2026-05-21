//! DynamoDB JSON wire-format types for rektifier.
//!
//! `AttributeValue` matches DynamoDB's on-wire shape (`{"S":"..."}`,
//! `{"N":"42"}`, etc). Operation request/response structs sit in
//! [`operations`] and use serde derives directly — they're trivial
//! PascalCase-renamed structs.

mod attribute_value;
pub mod ddl;
pub mod operations;

pub use attribute_value::{AttributeValue, Item};
pub use ddl::{
    AttributeDefinition, BillingModeSummary, CreateTableRequest, CreateTableResponse,
    DeleteGlobalSecondaryIndexAction, DeleteTableRequest, DeleteTableResponse,
    DescribeTableRequest, DescribeTableResponse, GlobalSecondaryIndex,
    GlobalSecondaryIndexDescription, GlobalSecondaryIndexUpdate, KeySchemaElement,
    ListTablesRequest, ListTablesResponse, LocalSecondaryIndex, LocalSecondaryIndexDescription,
    Projection, ProvisionedThroughput, ProvisionedThroughputDescription, StreamSpecification,
    TableClassSummary, TableDescription, Tag, UpdateGlobalSecondaryIndexAction,
    UpdateTableRequest, UpdateTableResponse,
};
pub use operations::{
    BatchGetItemRequest, BatchGetItemResponse, BatchWriteItemRequest, BatchWriteItemResponse,
    DeleteItemRequest, DeleteItemResponse, DeleteRequest, GetItemRequest, GetItemResponse,
    Get as TransactGet, KeysAndAttributes, PutItemRequest, PutItemResponse, PutRequest,
    QueryRequest, QueryResponse, ScanRequest, ScanResponse, TransactConditionCheck,
    TransactDelete, TransactGetItem, TransactGetItemResponse, TransactGetItemsRequest,
    TransactGetItemsResponse, TransactPut, TransactUpdate, TransactWriteItem,
    TransactWriteItemsRequest, TransactWriteItemsResponse, UpdateItemRequest, UpdateItemResponse,
    WriteRequest,
};
