// node index.js --pgUri tcp://djump-payment@localhost/djump_payment --mongoUri mongodb://localhost:27017/speakeasy_development_passenger_reports/ReportCollection --csvPath ~/Downloads/djump_users.csv

var async = require('async');
var MongoClient = require('mongodb').MongoClient;
var pg = require('pg');
var nopt = require('nopt');
var fs = require('fs');
var _ = require('lodash');

function parseMongoUri(uri) {
  var regex = /(.+\/\/.+\/.+)\/([^\/\?]+)(\?.+){0,1}$/;
  var match = regex.exec(uri);

  if (!match) {
    throw new Error('Malformed URI ' + uri);
  }

  var replicatSet = match[3] || '';

  return {
    dbUri: match[1] + replicatSet,
    collectionName: match[2],
    replicatSet: replicatSet
  };
}

function getData(mongoCollection, pgClient, callback) {
  var cursor = mongoCollection.find();
  var content = [];
  var titles = [
    "Facebook ID", "First Name", "Last Name",
    "Email", "Phone Number", "Postal Code", "Country", "Driver"
  ];

  content.push(titles);

  var retrieve = function () {
    cursor.nextObject(function (err, passengerReport) {
      if (err) {
        return callback(err);
      }

      if (passengerReport) {
        var formattedData = formatMongoData(passengerReport);

        getUserDataFromPg(pgClient, formattedData.uid, function (err, pgData) {
          if (err) return callback(err);

          if (pgData.length > 0) {
            var formattedPgData = formatPgData(pgData[0]);
            _.defaults(formattedData, formattedPgData);
          }

          var rowContent = [
            formattedData.facebookId,
            formattedData.firstName,
            formattedData.lastName,
            formattedData.email,
            formattedData.phoneNumber,
            formattedData.postalCode,
            formattedData.country,
            formattedData.driver
          ];

          content.push(rowContent);
          retrieve();
        });
      } else {
        callback(null, content);
      }
    });
  };

  retrieve();
}

function formatMongoData(mongoData) {
  var selectors = ['uid', 'name', 'email', 'phoneNumber', 'facebookId', 'driver'];
  var pickedData = _.pick(mongoData.data, selectors);
  var formattedData = {
    uid: pickedData.uid,
    email: pickedData.email,
    phoneNumber: pickedData.phoneNumber,
    facebookId: pickedData.facebookId,
    driver: _.isPlainObject(pickedData.driver)
  };
  if (_.isPlainObject(pickedData.name)) {
    formattedData.firstName = pickedData.name.firstName;
    formattedData.lastName = pickedData.name.lastName;
  }
  return formattedData;
}

function formatPgData(pgData) {
  var selectors = [
    'uid', 'firstName', 'lastName',
    'email', 'postalCode', 'phoneNumber', 'country'
  ];
  return _.pick(pgData, selectors);
}

function getUserDataFromPg(pgClient, uid, callback) {
  var query = 'SELECT * from "Users" where uid=$1;';
  var queryExecution = pgClient.query(query, [uid]);

  var pgContent = [];

  queryExecution.on('row', function (row) {
    pgContent.push(row);
  });

  queryExecution.on('error', function (err) {
    callback(err);
  });

  queryExecution.on('end', function(result) {
    callback(null, pgContent);
  });
}

function createCSVContent(data, callback) {
  var csvRow;
  var index;
  var csvContent = '';

  for (index = 0; index < data.length; index++) {
    csvRow = data[index].join(";");
    if (index < data.length) {
      csvRow += "\n";
    }
    csvContent += csvRow;
  }

  return csvContent;
}

function createCSVFile(content, path, callback) {
  fs.writeFile(path, content, callback);
}

function run(mongoUri, pgUri, csvFilePath, callback) {
  var parsedMongoUri = parseMongoUri(mongoUri);

  async.waterfall([
    function(next) {
      MongoClient.connect(parsedMongoUri.dbUri, next);
    },
    function(mongoDb, next) {
      mongoDb.createCollection(parsedMongoUri.collectionName, next);
    },
    function(mongoCollection, next) {
      var pgClient = new pg.Client(pgUri);
      pgClient.connect(function (err) {
        next(err, mongoCollection, pgClient);
      });
    },
    function(mongoCollection, pgClient, next) {
      getData(mongoCollection, pgClient, next);
    },
    function(data, next) {
      next(null, createCSVContent(data));
    },
    function(csvContent, next) {
      createCSVFile(csvContent, csvFilePath, next);
    }
  ], callback);
}

function displayHelp() {
  console.log('Usage: node index.js [options]');
  console.log('  --pgUri tcp://<user>@<ip>/<db>');
  console.log('  --mongoUri mongodb://<ip>:<port>/<db>/<collection>');
  console.log('  --csvPath <path>');
  console.log('  --help');
}

function isHelpNeeded(options) {
  return !(_.isString(options.pgUri) &&
    _.isString(options.mongoUri) &&
    _.isString(options.csvPath)) || options.help;
}

function extractData(options) {
  run(options.mongoUri, options.pgUri, options.csvPath, function (err) {
    if (err) {
      throw err;
    }
    process.exit(0);
  });
}

var knownOpts = {
  'pgUri': String,
  'mongoUri': String,
  'csvPath': String,
  'help': Boolean
};

var shortHands = {
  p: ['--pgUri'],
  m: ['--mongoUri'],
  c: ['--csvPath'],
  h: ['--help'],
  '?': ['--help']
};

var options = nopt(knownOpts, shortHands, process.argv, 2);

if (isHelpNeeded(options)) {
  displayHelp();
} else {
  extractData(options);
}
