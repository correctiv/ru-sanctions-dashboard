import $ from 'jQuery';

const META_URL =
  'https://correctiv.github.io/ru-sanctions-dashboard/src/data/meta.json';

const inject = (id, value) => $(`[data-id="${id}"]`).text(value || 0);

$.getJSON(META_URL, ({ all, recent, last_updated }) => {
  // last updated
  const lastUpdate = new Date(last_updated);
  inject('number9', lastUpdate.toLocaleString());

  // new sanctions
  inject('number12', recent.sanctions);

  // all sanctions
  inject('number13', all.sanctions);

  // persons number1
  inject('number1', recent.Person);

  // companies number2
  inject('number2', recent.Company);

  // other number3
  inject('number3', recent.Organization + recent.LegalEntity);

  // vessels number10
  inject('number10', recent.Vessel);

  // airplanes number11
  inject('number11', recent.Airplane);

  // USA number4
  inject('number4', recent.us);

  // EU number5
  inject('number5', recent.eu);

  // UK number6
  inject('number6', recent.gb);

  // UN number7
  inject('number7', recent.uno);

  // CH number8
  inject('number8', recent.ch);

  // AR number14
  inject('number14', recent.ar);

  // JP number15
  inject('number15', recent.jp);

  // AU number16
  inject('number16', recent.au);
});
