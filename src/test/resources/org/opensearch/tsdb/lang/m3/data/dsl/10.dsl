{
  "size" : 0,
  "query" : {
    "bool" : {
      "filter" : [
        {
          "term" : {
            "labels" : {
              "value" : "service:web-api",
              "boost" : 1.0
            }
          }
        },
        {
          "term" : {
            "labels" : {
              "value" : "name:requests_total",
              "boost" : 1.0
            }
          }
        },
        {
          "bool" : {
            "should" : [
              {
                "term" : {
                  "labels" : {
                    "value" : "region:us-east",
                    "boost" : 1.0
                  }
                }
              },
              {
                "term" : {
                  "labels" : {
                    "value" : "region:us-west",
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "minimum_should_match" : "1",
            "boost" : 1.0
          }
        },
        {
          "range" : {
            "min_timestamp" : {
              "from" : null,
              "to" : 1001000000,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        },
        {
          "range" : {
            "max_timestamp" : {
              "from" : 1000000000,
              "to" : null,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }
      ],
      "adjust_pure_negative" : true,
      "boost" : 1.0
    }
  },
  "aggregations" : {
    "0_unfold" : {
      "time_series_unfold" : {
        "min_timestamp" : 1000000000,
        "max_timestamp" : 1001000000,
        "step" : 100000,
        "stages" : [
          {
            "type" : "per_second"
          },
          {
            "type" : "aliasByTags",
            "tag_names" : [
              "service",
              "region"
            ]
          }
        ]
      }
    },
    "0_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "sort",
            "sortBy" : "avg",
            "sortOrder" : "desc"
          }
        ],
        "references" : {
          "0_unfold" : "0_unfold"
        },
        "inputReference" : "0_unfold"
      }
    }
  }
}
