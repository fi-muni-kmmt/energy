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
  let final = data;
  if (data.length > 2500) {
    final = data.slice((data.length % 2499), data.length)
  }
  console.log(final.length, country);
  fs.writeFileSync(`${__dirname}/results/${country}.json`, JSON.stringify(final, null, 2));
}))
