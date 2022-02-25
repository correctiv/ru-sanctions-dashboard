import $ from 'jQuery';

const META_URL =
  'https://correctiv.github.io/ru-sanctions-dashboard/src/data/meta.json';

$.getJSON(META_URL, (d) => {
  const lastUpdate = new Date(d.last_updated);
  console.log(lastUpdate);
});
