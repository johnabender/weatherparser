var express = require('express');

var app = express.createServer(express.logger());

var mongo = require('mongoskin');
var mongodb = require('mongodb');   // just to get the ObjectID type, use the skin for everything else
var ObjectID = require('mongodb').ObjectID;  // Get the objectID type


app.get('/', function(request, response) {
  response.send( process.env.MONGOHQ_URL || "hello, world" )
});

var port = process.env.PORT || 5000;
app.listen(port, function() {
  console.log("Listening on " + port);
});
