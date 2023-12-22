# Setup

* Set the following key in the Pinot broker's config:

  ```properties
  pinot.broker.query.rewriter.class.names=org.apache.pinot.sql.parsers.rewriter.CompileTimeFunctionsInvoker,org.apache.pinot.sql.parsers.rewriter.SelectionsRewriter,org.apache.pinot.sql.parsers.rewriter.PredicateComparisonRewriter,org.apache.pinot.sql.parsers.rewriter.ClpRewriter,org.apache.pinot.sql.parsers.rewriter.AliasApplier,org.apache.pinot.sql.parsers.rewriter.OrdinalsUpdater,org.apache.pinot.sql.parsers.rewriter.NonAggregationGroupByToDistinctQueryRewriter
  ```

* Set up your schema config as described
  [here](https://docs.pinot.apache.org/basics/data-import/clp#schema).
* Set up your table config as follows (ignore the settings in doc above). NOTE: This is not a
  complete table config since we omit irrelevant settings for brevity.

  ```json
  {
    "tableIndexConfig": {
      "streamConfigs": {
        "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.clplog.CLPLogMessageDecoder",
        "stream.kafka.decoder.prop.fieldsForClpEncoding": "<field>"
      },
      "noDictionaryColumns": [
        "<field>_dictionaryVars",
        "<field>_encodedVars",
        "<field>_logtype"
      ]
    },
    "fieldConfigList": [
      {
        "name": "<field>_logtype",
        "encodingType": "RAW",
        "indexType": "TEXT",
        "indexTypes": [
          "TEXT"
        ],
        "compressionCodec": "LZ4",
        "indexes": null
      },
      {
        "name": "<field>_dictionaryVars",
        "encodingType": "RAW",
        "indexType": "TEXT",
        "indexTypes": [
          "TEXT"
        ],
        "compressionCodec": "LZ4",
        "indexes": null
      },
      {
        "name": "<field>_encodedVars",
        "encodingType": "RAW",
        "indexTypes": [],
        "compressionCodec": "LZ4",
        "indexes": null
      }
    ]
  }
  ```
  
  * Essentially, in the settings above, we disable the dictionary for any CLP-encoded field's
    columns and add a TEXT index for the logtype and dictionaryVars columns of every CLP-encoded
    field.

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
