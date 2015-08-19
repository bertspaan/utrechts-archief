var fs = require('fs');
var path = require('path');
var url = require('url');
var config = require('histograph-config');
var request = require('request');
var _ = require('highland');

var geojson = {
  open: '{"type":"FeatureCollection","features":[',
  close: ']}'
};

var columns = ['Bron', 'Nummer', 'Code', 'Aanvrager', 'Handeling', 'Object', 'Redactionele vorm', 'Adres', 'Oorspronkelijk adres', 'Wijkaanduiding', 'Plaats', 'Land', 'Kadastraal nummer', 'Architect', 'Datering', 'Techniek', 'Aantal', 'Uiterlijke vorm', 'Afmetingen', 'Schaal', 'Notabene', 'Verwijzing', 'Datum notulen fabr', 'Nr notulen fabr', 'Orde', 'Geen vergun stukken', 'Gedig tekeningen'];

function geocode(obj, callback) {
  if (obj.Adres) {
    var queryString = obj.Adres;
    var params = 'search?related.id=bag/3295&related=hg:liesIn&type=hg:Street,hg:Address&q=' + queryString;
    var searchUrl = 'http://www.hetutrechtsarchief.nl/collectie/archiefbank/indexen/bouwdossiers/zoekresultaat?mivast=39&miadt=39&mizig=17&miview=ldt&milang=nl&micols=1&mires=0&mip2=&mip3=&mip4=&mip5=&mip6=&mibj=&miej=&mizk_alle=';

    request({
      url: url.resolve(config.api.baseUrl, params),
      json: true
    }, function (error, response, body) {
      if (body && body.features && body.features.length > 0) {
        var pits = body.features[0].properties.pits.filter(function(pit) {
          return pit.dataset === 'bag' || pit.dataset === 'nwb';
        })
        .map(function(pit) {
          var geometry;
          if (pit.geometryIndex > -1) {
            geometry = body.features[0].geometry.geometries[pit.geometryIndex];
          }
          return {
            type: 'Feature',
            properties: {
              url: searchUrl + [obj.Bron, obj.Nummer, obj.Code].join('+').replace(/ /g, '+').replace(/"/g, '').replace(/\+\-/g, '-'),
              id: pit.id,
              bron: obj.Bron.replace(/"/g, ''),
              nummer: obj.Nummer.replace(/"/g, ''),
              code: obj.Code.replace(/"/g, '')
            },
            geometry: geometry
          };
        });

        if (pits.length > 0) {
          callback(null, pits[0]);
        } else {
          callback();
        }
      } else {
        callback();
      }
    });
  } else {
    callback();
  }
}

var through = _.pipeline(
  _.split(),
  _.map(function(row) {
    var obj = {};
    var fields = row.split('💾');
    columns.forEach(function(column) {
      obj[column] = fields[columns.indexOf(column)];
    });
    return obj;
  }),
  _.map(function(obj) {
    return _.curry(geocode, obj);
  }),
  _.nfcall([]),
  _.series(),
  _.compact(),
  _.map(JSON.stringify),
  _.intersperse(',')
);

  var streams = _([
    _([geojson.open]),
    fs.createReadStream(path.join(__dirname, 'data', 'Totaalbestand bouwtekeningen Utrecht 1853-1995.csv'), {
      encoding: 'utf8'
    }).pipe(through),
    _([geojson.close])
  ]);

streams.sequence()
  .pipe(process.stdout);
