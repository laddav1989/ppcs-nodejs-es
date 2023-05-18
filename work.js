'use strict'

require('dotenv').config()

const { Client } = require('@elastic/elasticsearch')

const client = new Client({
  nodes: [
    'http://'+process.env.ES_HOST+':'+process.env.ES_PORT
  ]
})

async function run () {
  const { body } = await client.search({
    index: 'offerstat',
    body: {
      query: {
        match: {
          webshopId: 590
        }
      }
    }
  })

  console.log(body.hits.hits)
}

run().catch(console.log)