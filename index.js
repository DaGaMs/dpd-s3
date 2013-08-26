var knox = require('knox')
  , Resource = require('deployd/lib/resource')
  , httpUtil = require('deployd/lib/util/http')
  , formidable = require('formidable')
  , fs = require('fs')
  , util = require('util')
  , path = require('path');

function S3Bucket(name, options) {
  Resource.apply(this, arguments);
  if (this.config.key && this.config.secret && this.config.bucket) {
    this.client = knox.createClient({
        key: this.config.key
      , secret: this.config.secret
      , bucket: this.config.bucket
    });
  }
}
util.inherits(S3Bucket, Resource);
module.exports = S3Bucket;
S3Bucket.label = "S3 Bucket";

S3Bucket.prototype.clientGeneration = true;

S3Bucket.events = ["uploading", "uploaded", "get", "delete"];
S3Bucket.basicDashboard = {
  settings: [{
      name: 'bucket'
    , type: 'string'
  }, {
      name: 'key'
    , type: 'string'
  }, {
      name: 'secret'
    , type: 'string'
  }]
};

S3Bucket.prototype.handle = function (ctx, next) {
  var req = ctx.req
    , bucket = this
    , domain = {url: ctx.url};

  if (!this.client) return ctx.done("Missing S3 configuration!");

  if (req.method === "POST" && !req.internal && req.headers['content-type'].indexOf('multipart/form-data') === 0) {
    var form = new formidable.IncomingForm();
    var remaining = 0;
    var files = [];
    var error;

    var uploadedFile = function(err, res, domain) {
        var done = function (err2) {
            if (err2) {
                error = err2;
                return ctx.done(err2);
            } else {
                remaining--;
                if (remaining <= 0) {
                    if (req.headers.referer) {
                        httpUtil.redirect(ctx.res, req.headers.referer || '/');
                    } else {
                        ctx.done(null, files);
                    }
                }
            }
        }
        if (bucket.events.uploaded) {
            domain.s3response = res;
            domain.s3error = err;
            bucket.events.uploaded.run(ctx, domain, function (err) {
                done(err);
            });
        } else {
            done(err);
        }
    };

    form.parse(req)
      .on('file', function(name, file) {
        remaining++;
        if (bucket.events.uploading) {
          bucket.events.uploading.run(ctx, {url: ctx.url, fileSize: file.size, fileName: file.name}, function(err) {
            if (err) return uploadedFile(err);
            bucket.uploadFile(file.name, file.size, file.mime, file.path, uploadedFile);
          });
        } else {
          bucket.uploadFile(file.name, file.size, file.mime, file.path, uploadedFile);
        }
      })
      .on('error', function(err) {
        ctx.done(err);
        error = err;
      });
    req.resume();
    return;
  }

  if (req.method === "POST" || req.method === "PUT") {
    
    domain.fileSize = ctx.req.headers['content-length'];
    domain.fileName = path.basename(ctx.url);
    
    var finally = function (err, res) {
        if (bucket.events.uploaded) {
            domain.s3response = res;
            domain.s3error = err;
            bucket.events.uploaded.run(ctx, domain, function (err) {
                if (err) return ctx.done(err);
                else return ctx.done(err, res);
            });
        } else {
            ctx.done(err, res);
        }
    };
    if (this.events.uploading) {
      this.events.uploading.run(ctx, domain, function(err) {
        if (err) return ctx.done(err);
        bucket.upload(ctx, finally);
      });
    } else {
      this.upload(ctx, finally);
    }

  } else if (req.method === "GET") {
    if (ctx.res.internal) return next(); // This definitely has to be HTTP.

    if (this.events.get) {
      this.events.get.run(ctx, domain, function(err) {
        if (err) return ctx.done(err);
        bucket.get(ctx, next);
      });
    } else {
      this.get(ctx, next);
    }

  } else if (req.method === "DELETE") {
    
    if (this.events['delete']) {
      this.events['delete'].run(ctx, domain, function(err) {
        if (err) return ctx.done(err);
        bucket.del(ctx, next);
      }); 
    } else {
      this.del(ctx, next);
    }
  } else {
    next();
  }
};

S3Bucket.prototype.uploadFile = function(filename, filesize, mime, file, fn) {
  var bucket = this;
  var headers = {
      'Content-Length': filesize
    , 'Content-Type': mime
    , 'x-amz-acl': 'public-read'
  };
  
  this.client.putFile(file, filename, headers, function(err, res) { 
    if (err) return ctx.done(err);
    if (res.statusCode !== 200) {
      bucket.readStream(res, function(err, message) {
        fn(err || message, null, {"fileName": filename, "fileSize": filesize});
      });
    } else {
      fn(err, res, {"fileName": filename, "fileSize": filesize});
    }
  });
};

S3Bucket.prototype.upload = function(ctx, next) {
  var bucket = this
    , req = ctx.req;

  var headers = {
      'Content-Length': req.headers['content-length']
    , 'Content-Type': req.headers['content-type']
    , 'x-amz-acl': 'public-read'
  };

  this.client.putStream(req, ctx.url, headers, function(err, res) { 
    if (err) return ctx.done(err);
    if (res.statusCode !== 200) {
      bucket.readStream(res, function(err, message) {
        next(err || message);
      });
    } else {
      next(err, res);
    }
  });
  req.resume();
};

S3Bucket.prototype.get = function(ctx, next) {
  var bucket = this;
  var url = 'https://' + this.config.bucket + '.s3.amazonaws.com' + ctx.url;

  httpUtil.redirect(ctx.res, url);
};

S3Bucket.prototype.del = function(ctx, next) {
  var bucket = this;

  this.client.deleteFile(ctx.url, function(err, res) {
    if (err) ctx.done(err);
    if (res.statusCode !== 200) {
      bucket.readStream(res, function(err, message) {
        ctx.done(err || message);
      });
    } else {
      ctx.done();
    }
  });
};

S3Bucket.prototype.readStream = function(stream, fn) {
  var buffer = '';
  stream.on('data', function(data) {
    buffer += data;
  }).on('end', function() {
    fn(null, buffer);
  }).on('error', function(err) {
    fn(err);
  });
};
