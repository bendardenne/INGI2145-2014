var express = require('express');
var assert = require('assert');
var app = require('./app.js');
var ejs = require('ejs');
var fs = require('fs');
var AM = require('./account-manager.js');
var router = express.Router();


////////////////////////////////////////////////////////////////////////////////

function setDisplayDate(tweetsToDisplay) {
    tweetsToDisplay.forEach(function(tweet) {
        tweet.display_time = new Date(tweet.created_at).toString();
    });
    return tweetsToDisplay;
}

// NOTE(norswap): This method is necessary because, annoyingly, EJS does not
//  support dynamic includes (including a file whose name is passed via the
//  dictionary -- it has to be hardcoded in the template instead).
function render(res, dict) {
    fs.readFile('views/'+ dict.partial + '.ejs', 'utf-8', function(err, data) {
        assert(!err);
        dict.partial = ejs.render(data, dict);
        res.render('template', dict);
    });
}

////////////////////////////////////////////////////////////////////////////////
// Login page

router.get('/', function(req, res) {
   
    // If user is logged in, redirect to home 
    if (req.session.user != null) {
        res.redirect('/home');
        return;
    }
    
    // else render login
    res.render('login');
});

////////////////////////////////////////////////////////////////////////////////
// Login / Logout

router.post('/validate', function(req, res) {
    AM.manualLogin(
        req.body.username,
        req.body.password,
        app.users,
    function(err, user) {
        res.setHeader('content-type', 'application/json');
                
        if (!user) {
            res.statusCode = 403;
            var o = {message: err};
            res.send(JSON.stringify(o));
            return;
        }

        req.session.user = user;
        var fullUrl = req.protocol + '://' + req.get('host') + '/home';
        var o = {message: 'OK', url: fullUrl}
        res.send(JSON.stringify(o));
    });
});

router.post('/logout', function(req, res) {
    req.session.destroy();
    res.redirect('/');
});

////////////////////////////////////////////////////////////////////////////////
// User Profile

router.get('/usr/:username', function(req, res) {
    
    if (req.session.user == null) {
        // if user is not logged in redirect back to login page
        res.redirect('/');
        return;
    }
   
    // get the user 
    app.users.findOne({username: req.params.username}, function(e, o) { 
        
        //get their 10 latest tweets
        app.tweets.find({username: req.params.username})
            .limit(10)
            .toArray(function(err, array) {
                render(res, { partial: 'profile', 
                    title: o.username + "'s profile", 
                    username: o.username, 
                    following : o.followers.indexOf(req.session.user.username) > -1, 
                    tweets: setDisplayDate(array)});
            });
    });
});

router.get('/usr/:username/following', function(req, res) {
    if (req.session.user == null) {
        res.redirect('/');
        return;
    }
    
    //get who they're following
    app.users.findOne({username: req.params.username}, {following: 1}, function(e, o) { 
                render(res, { partial: 'follow', 
                    title:"Users " + req.params.username + " is following", 
                    follow : o.following});
            });
});

router.get('/usr/:username/followers', function(req, res) {
    if (req.session.user == null) {
        res.redirect('/');
        return;
    }
    
    //get who's follwing them
    app.users.findOne({username: req.params.username}, {followers: 1}, function(e, o) { 
                render(res, { partial: 'follow', 
                    title: req.params.username + "'s followers", 
                    follow : o.followers});
            });
});

router.post('/follow', function(req, res) {
    if (req.session.user == null) {
        res.statusCode = 403;
        var o = {message: "You are not logged in."};
        res.send(JSON.stringify(o));
        return;
    }
    
    // req.params.username is followed by us, add ourselves in the array
    app.users.update({username: req.body.target_username}, {$addToSet : {followers: req.session.user.username}}, function(err, result){
        if(result == 0) {
            res.statusCode = 403;
            var o = {message: "No such user"};
            res.send(JSON.stringify(o));
            return;
        }   
       
        // we follow req.params.username 
        app.users.update({username: req.session.user.username}, {$addToSet : {following: req.body.target_username}}, function(){}); 
        res.redirect('/usr/' + req.body.target_username);
        return;
    }); 
    
});

router.post('/unfollow', function(req, res) {
    if (req.session.user == null) {
        res.statusCode = 403;
        var o = {message: "You are not logged in."};
        res.send(JSON.stringify(o));
        return;
    }

    // req.params.username is followed by us
    app.users.update({username: req.body.target_username}, {$pull : {followers: req.session.user.username}}, function(err, result){
        assert(err == null);
         
        // we follow req.params.username
        app.users.update({username: req.session.user.username}, {$pull : {following: req.body.target_username}}, function(){}); 
    }); 
    
    res.redirect('/usr/' + req.body.target_username);
    return;
});

////////////////////////////////////////////////////////////////////////////////
// User Timeline

router.get('/home', function(req, res) {
    if (req.session.user == null) {
        // if user is not logged in redirect back to login page
        res.redirect('/');
        return;
    }

    app.users.findOne({username: req.session.user.username}, {following: 1}, function(e, o) {
        // 20 most recent tweets
        followingTweets = app.tweets.find({username: {$in: o.following}}).sort({created_at: -1}).limit(20);
    
        followingTweets.toArray(function(err, array) {
            render(res, { partial: 'home', 
                        title: req.session.user.name + "'s feed", 
                        tweets: setDisplayDate(array)});
        });    
    });
});

////////////////////////////////////////////////////////////////////////////////
// User Timeline
router.post('/newTweet', function(req, res) {
    
    res.setHeader('content-type', 'application/json');
    if (req.session.user == null) {
        res.statusCode = 403;
        var o = {message: "You are not logged in."};
        res.send(JSON.stringify(o));
        return;
    }

    var tweet = req.body.text;
    if(tweet.length > 140) {
        res.statusCode = 403;
        var r = {message: "Your tweet is too long." };
        res.send(JSON.stringify(r));
        return;
    }
    
    t = {text: tweet, created_at: new Date(), username: req.session.user.username, 
        name: req.session.user.name};

    app.tweets.insert(t, {safe:true}, function(){});
 
    // send the tweet to spark via kafka
    app.producer.send([ {topic: "tweets", messages: tweet }  ], function(err, data) {});
    
    res.statusCode = 200;
    var r = {message: "Tweet posted" };
    res.send(JSON.stringify(r));
    return;
});

////////////////////////////////////////////////////////////////////////////////

module.exports = router;
