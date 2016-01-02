var zlib = require('zlib'),
    async = require('async'),
    aws = require('aws-sdk'),
    ua = require('universal-analytics'),
    config = require('./config'),
    s3 = new aws.S3({
        region: 'us-east-1'
    });

var bucket = config.bucket,
    googleAnalyticsId = config.googleAnalyticsId,
    regex = /(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+\S+)\t(\S+)\t(\S+)\t(\S+)\t(\S+)/,
    map = {
        1: 'date',
        2: 'time',
        3: 'x_edge_location',
        4: 'bytes_sent',
        5: 'client_ip',
        6: 'verb',
        7: 'server_host',
        8: 'uri',
        9: 'status_code',
        10: 'referrer',
        11: 'user_agent',
        12: 'uri_query',
        13: 'cookie',
        14: 'x_edge_result_type',
        15: 'x_edge_request_id',
        16: 'x_host_headers',
        17: 'protocol',
        18: 'bytes_recieved',
        19: 'time_taken',
        20: 'x_forwarded_for',
        21: 'ssl_protocol',
        22: 'ssl-cipher',
        23: 'x_edge_response_result_type'
    };

exports.handler = function(event, context) {
    // Read options from the event.
    var key = event.Records[0].s3.object.key;

    async.waterfall([
        getLogFileFromS3.bind(this, bucket, key),
        zlib.unzip,
        parseLogsIntoObjects,
        sendDataToGoogleAnalytics,
        deleteLogFile.bind(this, bucket, key)
    ], context.done);
}

function getLogFileFromS3 (bucket, key, callback) {
    s3.getObject({
        Bucket: bucket,
        Key: key
    }, function (error, data) {
        if (error) {
            setImmediate(callback, error);
            return;
        }
        callback(null, data.Body);
    });
}

function parseLogsIntoObjects (data, callback) {
    var lines = data.toString().split("\n");
    async.map(lines, function(line, cb) {
        var matches = line.match(regex),
            ret = {};

        if (!matches) {
            setImmediate(cb);
            return;
        }

        for (var k in map) {
            var v = map[k];
            ret[v] = matches[k] !== '-' ? matches[k] : null;
        }

        cb(null, ret);
    }, function (error, parsed) {
        if (error) {
            setImmediate(callback, error);
            return;
        }
        callback(null, parsed);
    });
}

function sendDataToGoogleAnalytics (logs, callback) {
    async.each(logs, function (log, cb) {
        // Dont log options
        if (!log || log.verb === 'OPTIONS') {
            setImmediate(cb);
            return;
        }
        var visitor = ua(googleAnalyticsId);
        visitor.pageview({
            dp: log.uri + (log.uri_query ? '?' + log.uri_query : ''),
            dh: log.x_host_headers,
            uip: log.x_forwarded_for || log.client_ip,
            //For some reason, its encoded twice
            ua: decodeURIComponent(decodeURIComponent(log.user_agent)),
            dr: log.referrer
        }).send(cb);
    }, function (error, data) {
        callback();
    });
}

function deleteLogFile (bucket, key, callback) {
    s3.deleteObject({
      Bucket: bucket,
      Key: key
    }, callback);
}
