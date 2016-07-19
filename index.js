var request = require('request'),
    gammautils = require('gammautils'),
    async = require('async');

function getMessageBody(message) {
    if(typeof message === 'object') {
        return JSON.stringify(message);
    }

    return message;
}

function HerokuSqsProducer(options) {
    this._sqs = options.sqs;
    this._queueUrl = options.queueUrl;
    this._heroku = options.heroku;
}

HerokuSqsProducer.prototype.startFormation = function (sqsReceipts, callback) {
    if(typeof sqsReceipts === 'function') {
        callback = sqsReceipts;
        sqsReceipts = {};
    }

    var heroku = this._heroku;

    request({
        method: 'PATCH',
        url: 'https://api.heroku.com/apps/' + heroku.app + '/formation/' + heroku.processType,
        headers: {
            Accept: 'application/vnd.heroku+json; version=3',
            Authorization: 'Bearer ' + heroku.token
        },
        body: JSON.stringify({
            quantity: heroku.formation.quantity,
            size: heroku.formation.size
        })
    }, function(err, res, body) {
        if(err) {
            return callback(err);
        }

        if(res.statusCode !== 200) {
            return callback(new Error('Heroku returned code ' + res.statusCode));
        }

        var formation = JSON.parse(body);

        callback(null, {
            sqsReceipts: sqsReceipts,
            formation: {
                quantity: formation.quantity,
                size: formation.size
            }
        });
    });
}

HerokuSqsProducer.prototype.awakeDynos = function (sqsReceipts, callback) {
    if(typeof sqsReceipts === 'function') {
        callback = sqsReceipts;
        sqsReceipts = {};
    }

    var _this = this,
        heroku = this._heroku;

    request({
        method: 'GET',
        url: 'https://api.heroku.com/apps/' + heroku.app + '/formation/' + heroku.processType,
        headers: {
            Accept: 'application/vnd.heroku+json; version=3',
            Authorization: 'Bearer ' + heroku.token
        }
    }, function(err, res, body) {
        if(err) {
            return callback(err);
        }

        if(res.statusCode !== 200) {
            return callback(new Error('Heroku returned code ' + res.statusCode));
        }

        var formation = JSON.parse(body);

        if(formation.quantity === 0) {
            return _this.startFormation(sqsReceipts, callback);
        }

        callback(null, {
            sqsReceipts: sqsReceipts,
            formation: {
                quantity: formation.quantity,
                size: formation.size
            }
        });
    });
}

HerokuSqsProducer.prototype.sendMessages = function (messages, callback) {
    if(!Array.isArray(messages)) {
        messages = [messages];
    }

    messages = messages.map(function (message) {
        return {
            Id: gammautils.string.generateGuid(),
            MessageBody: getMessageBody(message)
        };
    });

    var _this = this,
        messageBatch = gammautils.array.chop(messages, 10),
        sqs = this._sqs,
        queueUrl = this._queueUrl;

    async.eachSeries(messageBatch, function (messages, cb) {
        sqs.sendMessageBatch({
            Entries: messages,
            QueueUrl: queueUrl
        }, function (err, data) {
            if(err) {
                return cb(err);
            }

            setTimeout(function () {
                cb(null, data);
            }, 250);
        });
    }, function (err, sqsReceipts) {
        if(err) {
            return callback(err);
        }

        _this.awakeDynos(sqsReceipts, callback);
    });
}

HerokuSqsProducer.prototype.sendMessage = HerokuSqsProducer.prototype.sendMessages

module.exports = HerokuSqsProducer;