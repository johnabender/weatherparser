var express = require('express');

var app = express.createServer(express.logger());


var mongo = require('mongoskin');
var mongodb = require('mongodb');   // just to get the ObjectID type, use the skin for everything else
var ObjectID = require('mongodb').ObjectID;  // Get the objectID type

var conString = process.env.MONGOHQ_URL

var db = mongo.db(conString, {
     auto_reconnect: true,
     poolSize: 5,
     safe: false
});


app.get('/', function(request, response) {
    db.collection( 'currentForecast' ).find( request.query ).toArray( function( err, result )
    {
        if (err) { response.send(err) }
        else { response.send(JSON.stringify(result)) }
    })
});

var port = process.env.PORT || 5000;
app.listen(port, function() {
  console.log("Listening on " + port);
});

process.on('SIGINT', function() {
    console.log('Received SIGINT');
    db.close(function(){
        console.log('weatherparser server has closed its database');
    });
});
