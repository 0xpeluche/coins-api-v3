const { getClient } = require("../db/elastic");


async function fetchAllRecords(index) {
  const allRecords = [];
  const client = getClient();
  let response = await client.search({
    index: index,
    scroll: '1m',
    body: {
      query: {
        match_all: {}
      },
      size: 10000
    }
  });

  while (response.hits.hits.length) {
    allRecords.push(...response.hits.hits);
    console.log(response._scroll_id)
    console.log(`Fetched ${allRecords.length} records`, response.hits.hits.length);
    response = await client.scroll({
      scroll_id: response._scroll_id,
      scroll: '1m'
    });
  }

  return allRecords;
}

fetchAllRecords('coins-metadata-test')
  .then(records => console.log(`Fetched ${records.length} records`))
  .catch(err => console.error('Error fetching records:', err));
