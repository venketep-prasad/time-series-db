{
  "size" : 0,
  "query" : {
    "bool" : {
      "filter" : [
        {
          "term" : {
            "labels" : {
              "value" : "city_name:San Francisco",
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
                    "value" : "host:host1",
                    "boost" : 1.0
                  }
                }
              },
              {
                "term" : {
                  "labels" : {
                    "value" : "host:host2",
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
              "from" : 989200000,
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
        "min_timestamp" : 989200000,
        "max_timestamp" : 1001000000,
        "step" : 100000,
        "stages" : [
          {
            "type" : "sum",
            "group_by_labels" : [
              "merchantID"
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
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 60000,
            "function" : "sum"
          },
          {
            "type" : "alias",
            "pattern" : "my     # alias"
          },
          {
            "type" : "moving",
            "interval" : 10800000,
            "function" : "sum"
          },
          {
            "type" : "keepLastValue",
            "look_back_window" : 7200000
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
