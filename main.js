const csv = require('csvtojson');
const fs = require('fs');
const sourceFolder = `${__dirname}/sources`;

const countries = fs.readdirSync(sourceFolder);
const finished = Promise.all(countries.map(async (country) => {
  const jsonArray = await csv().fromFile(`${sourceFolder}/${country}`);
  const data = jsonArray.reduce((acc, curr, index) => {
    const currIndex = Math.floor(index / 24);
    acc[currIndex] = { ...acc[currIndex] || curr, price: ((acc[currIndex]?.price || 0) + curr.price)}
    return acc;
  }, []).map(({ price, ...rest }) => ({ ...rest, price: Math.round((price / 24 * 100))/100 }));
  // let final = data;
  // if (data.length > 2500) {
  //   final = data.slice((data.length % 2499), data.length)
  // }
  // console.log(final.length, country);
  // fs.writeFileSync(`${__dirname}/results/${country}.json`, JSON.stringify(final, null, 2));
  return data;
}));

finished.then(([first, ...data]) => {
  console.log(first.length);
  const result = data.reduce((acc, curr) => {
    return acc.map((item) => {
      const found = curr.find(({ 'Datetime (UTC)': time }) => time === item.time) || {Country: curr[0].Country, 'Price (EUR/MWhe)': null};
      return { ...item, [found.Country.toLowerCase()]: found['Price (EUR/MWhe)'] }
    });
  }, first.map((item) => ({ time: item['Datetime (UTC)'], [item.Country.toLowerCase()]: item['Price (EUR/MWhe)'] })));
  console.log(result.length);
  const final = result.slice((result.length % 2499), result.length);
  console.log(final.length);
  fs.writeFileSync(`${__dirname}/results/joined.json`, JSON.stringify(final, null, 2));
  const perChunk = 500 // items per chunk    

  const chunked = final.reduce((resultArray, item, index) => { 
    const chunkIndex = Math.floor(index/perChunk)

    if(!resultArray[chunkIndex]) {
      resultArray[chunkIndex] = [] // start a new chunk
    }

    resultArray[chunkIndex].push(item)

    return resultArray
  }, []);
  chunked.map((item, index) => {
    fs.writeFileSync(`${__dirname}/results/chunked-${index}.json`, JSON.stringify(item, null, 2));
  })
})
