# Setup

* Set the following key in the Pinot broker's config:

  ```properties
  pinot.broker.query.rewriter.class.names=org.apache.pinot.sql.parsers.rewriter.CompileTimeFunctionsInvoker,org.apache.pinot.sql.parsers.rewriter.SelectionsRewriter,org.apache.pinot.sql.parsers.rewriter.PredicateComparisonRewriter,org.apache.pinot.sql.parsers.rewriter.ClpRewriter,org.apache.pinot.sql.parsers.rewriter.AliasApplier,org.apache.pinot.sql.parsers.rewriter.OrdinalsUpdater,org.apache.pinot.sql.parsers.rewriter.NonAggregationGroupByToDistinctQueryRewriter
  ```

# Using CLP_MATCH

* `CLP_MATCH`'s signature is:

  ```sql
  clpMatch("columnGroupName", '<wildcard-query>')
  ```
  
  OR

  ```sql
  clpMatch("columnGroupName_logtype", "columnGroupName_dictionaryVars",
           "columnGroupName_encodedVars", 'wildcardQuery')
  ```
  
  where:
  * `columnGroupName` is the name of the field configured in the
    `CLPLogRecordExtractorConfig.fieldsForClpEncoding`.
  * `wildcardQuery` is the query to use to filter records. Two wildcards are supported:
    * `*` - matches zero or more characters 
    * `?` - matches any single character
