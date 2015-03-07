var fs = require('fs');
var byline = require('byline');
var mongo = require('mongodb').MongoClient;
var assert = require('assert');
var AM = require('./account-manager.js');

mongo.connect('mongodb://localhost:27017/twitter', function(err, db) {

  assert.equal(null, err);
  var lineCount     = 0;
  var readAllLines  = false;

  users = db.collection('users');
  users.remove({}, function(){});
  tweets = db.collection('tweets');
  tweets.remove({}, function(){});	  
    
    users.dropAllIndexes(function() {});
    
    db.ensureIndex("users", { username: 1}, function(err, indexname) {
            assert.equal(null, err);
        }); 

  var semaphore = 1;
  function callback(err) {
      --semaphore;
	
      if (semaphore !== 0) return;
        readAllLines = true;
  }
  var u = byline(fs.createReadStream(__dirname + '/users.json'));

  u.on('data', function(line) {
    try {
      lineCount++;
      var obj = JSON.parse(line);
      
      AM.addNewAccount(obj, users, function () {
        if(--lineCount === 0 && readAllLines){ 
          db.close();
        }
      });

    } catch (err) {
      console.log("Error:", err);
    }
  });
  u.on('end', callback);

  var t = byline(fs.createReadStream(__dirname + '/sample.json'));
  lineCount = 0;
  t.on('data', function(line) {
    try {
      lineCount++;
      var obj = JSON.parse(line);
      
      tweets.insert(obj, {w:1}, function() {
      if(--lineCount === 0 && readAllLines){ 
        db.close();
      }});
    } catch (err) {
      console.log("Error:", err);
    }
  });
  t.on('end', callback);

});
