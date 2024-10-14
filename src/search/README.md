## KQIR (a.k.a. Kvrocks Search): Kvrocks Query Intermediate Representation

Here, *KQIR* refers to both
- the multiple-level *query intermediate representation* for Apache Kvrocks, and
- the *architecture and toolset* for the query optimization and execution.

Note that when interacting with users, we also refer to KQIR as *Kvrocks Search* to prevent overwhelming with excessive technical information.

### Architecture

![Architecture of KQIR](../../assets/KQIR.png)

### Components

- User Interface: both SQL and RediSearch Query syntax is supported to be the frontend language of KQIR
  - SQL Parser: A parser that accepts an extended subset of MySQL syntax
  - Redis Query Parser: A parser that accepts [RediSearch query syntax](https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/query_syntax/) (only DIALECT 2 or greater is planned to be supported)
- KQIR: a multiple level query intermediate representation, currently including two levels (syntactical IR and planning IR)
  - Syntactical IR: A high level IR that syntactically represents the query language
  - Planning IR: A low level IR that represents plan operators for query execution
- KQIR passes: analysis and transformation procedures on KQIR
  - Semantic Checker: to check if there is any semantic errors in the IR
  - Expression Passes: passes for query expressions, especially for logical/boolean expressions
  - Numeric Passes: passes for numeric & arithmetic expressions utilizing math properties
  - Planning Passes: passes on the plan operators for scheduling a better execution plan
  - Pass Manager: to manage the pass execution sequence and order
  - Cost Model: to analyze the cost for the current plan, used by some planning passes
- Plan Executor: a component for query execution via the iterator model
- Indexer: to perform the indexing for various types of fields during data changes

### References

- [KQIR: a query engine for Apache Kvrocks that supports both SQL and RediSearch queries](https://kvrocks.apache.org/blog/kqir-query-engine)
- [Index encoding format for Kvrocks Search](https://kvrocks.apache.org/community/kvrocks-search-index-encoding)
