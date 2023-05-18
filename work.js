'use strict'

require('dotenv').config()

const { Client } = require('@elastic/elasticsearch')

const client = new Client({
  nodes: [
    'http://'+process.env.ES_HOST+':'+process.env.ES_PORT
  ]
})

async function run () {

  /*
  const { body } = await client.search({
    index: 'offerstat',
    body: {
      query: {
        match: {
          webshopId: 590
        }
      },
      size: 10000
    }
  })

  console.log(body.hits.hits)
  */

  var webshopId = 590;
  var startDate = '2023-04-18';
  var endDate = '2023-05-18';
  var pageStart = 0;
  var pageStop = 100;

  const { body } = await client.search({
    index: 'offerstat',
    body: {
      "size": 0,
      "aggs": {
        "f": {
          "filter": {
            "bool": {
              "must": [
                {
                  "term": {
                    "webshopId": webshopId
                  }
                },
                {
                  "term": {
                    "active": true
                  }
                },
                {
                  "exists": {
                  "field": "frontendDatasheetId"
                }
              }
            ]
          }
        },
        "aggs": {
          "by_offer": {
            "terms": {
              "script": "doc['name.keyword'].value + '; offerId: ' + doc['offerId'].value",
                "size": 10000,
                "order": {
                  "_term": "asc"
                }
              },
              "aggs": {
                "stats": {
                  "nested": {
                    "path": "stat"
                  },
                  "aggs": {
                    "stat": {
                      "filter": {
                        "bool": {
                          "filter": [
                            {
                              "range": {
                                "stat.date": {
                                  "gte": startDate,
                                  "lte": endDate
                                }
                              }
                            }
                          ]
                        }
                      },
                      "aggs": {
                        "sum_olcsobbatCt": {
                          "sum": {
                            "field": "stat.olcsobbatCt"
                          }
                        },
                        "sum_olcsobbatCredit": {
                          "sum": {
                            "field": "stat.olcsobbatCredit"
                          }
                        },
                        "sum_clicks": {
                          "sum": {
                            "field": "stat.clicks"
                          }
                        },
                        "sum_conversions": {
                          "sum": {
                            "field": "stat.conversions"
                          }
                        },
                        "sum_conversionValues": {
                          "sum": {
                            "field": "stat.conversionValues"
                          }
                        },
                        "sum_cost": {
                          "sum": {
                            "field": "stat.cost"
                          }
                        },
                        "sum_credit": {
                          "sum": {
                            "field": "stat.credit"
                          }
                        },
                        "sum_impressions": {
                          "sum": {
                            "field": "stat.impressions"
                          }
                        },
                        "avg_clickRatio": {
                          "avg": {
                            "field": "stat.clickRatio"
                          }
                        },
                        "avg_cpc": {
                          "avg": {
                            "field": "stat.cpc"
                          }
                        },
                        "avg_ROAS": {
                          "avg": {
                            "field": "stat.ROAS"
                          }
                        },
                        "avg_CPA": {
                          "avg": {
                            "field": "stat.CPA"
                          }
                        }
                      }
                    }
                  }
                },
                "avg_clickRatio": {
                  "bucket_script": {
                    "buckets_path": {
                      "sumclicks": "stats>stat>sum_clicks",
                      "sumimpressions": "stats>stat>sum_impressions"
                    },
                    "script": "(params.sumimpressions > 0 ? (params.sumclicks \/ params.sumimpressions) : 0)"
                  }
                },
                "avg_cpc": {
                  "bucket_script": {
                    "buckets_path": {
                      "sumcredit": "stats>stat>sum_credit",
                      "sumclicks": "stats>stat>sum_clicks"
                    },
                    "script": "(params.sumclicks > 0 ? (params.sumcredit \/ params.sumclicks) : 0)"
                  }
                },
                "avg_ROAS": {
                  "bucket_script": {
                    "buckets_path": {
                      "sumconversionValues": "stats>stat>sum_conversionValues",
                      "sumcredit": "stats>stat>sum_credit"
                    },
                    "script": "(params.sumcredit > 0 ? (params.sumconversionValues \/ params.sumcredit) : 0)"
                  }
                },
                "avg_CPA": {
                  "bucket_script": {
                    "buckets_path": {
                      "sumcredit": "stats>stat>sum_credit",
                      "sumconversions": "stats>stat>sum_conversions"
                    },
                    "script": "(params.sumconversions > 0 ? (params.sumcredit \/ params.sumconversions) : 0)"
                  }
                },
                "paging": {
                  "bucket_sort": {
                    "sort": [],
                    "from": pageStart,
                    "size": pageStop
                  }
                }
              }
            }
          }
        }
      }
    }
  })
  
  console.log(body.aggregations.f.by_offer)
  //stats.stat
}

run().catch(console.log)