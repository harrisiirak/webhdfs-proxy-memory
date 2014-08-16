'use strict';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;

var WebHDFSProxy = require('webhdfs-proxy');
var WebHDFS = require('webhdfs');

var fs = require('fs');
var demand = require('must');
var sinon = require('sinon');

var handler = sinon.spy(require('../'));

function matchLastCallParams (params, path) {
  var args = handler.args[handler.args.length - 1];

  // Sinon matchers doesn't work here, do it manually
  for (var key in params) {
    if (!params.hasOwnProperty(key)) {
      return false;
    }

    if (params[key] !== params[key]) {
      return false;
    }
  }

  if (path !== undefined && args[1] !== path) {
    return false;
  }

  return true;
}

describe('WebHDFS proxy memory storage middleware', function () {
  // Setup WebHDFS client
  var proxyServer = null;
  var client = null

  // Set options
  var path = '/files/' + Math.random();
  var opts = {
    path: '/webhdfs/v1',
    http: {
      port: 45000
    }
  };

  before(function (done) {
    client = WebHDFS.createClient({
      user: 'webuser',
      host: 'localhost',
      port: 45000,
      path: '/webhdfs/v1'
    });

    proxyServer = WebHDFSProxy.createServer(opts, handler, done);
  });

  it('should make a directory', function (done) {
    client.mkdir(path, function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'mkdirs', permissions: '0777' }, path)).be.truthy();

      return done();
    });
  });

  it('should return an error if directory already exists', function (done) {
    client.mkdir(path, function (err) {
      demand(err).be.not.null();
      demand(matchLastCallParams({ op: 'mkdirs', permissions: '0777' }, path)).be.truthy();

      return done();
    });
  });

  it('should create and write data to a file', function (done) {
    client.writeFile(path + '/file-1', 'random data', function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'create' }, path + '/file-1')).be.truthy();

      done();
    });
  });

  it('should append content to an existing file', function (done) {
    client.appendFile(path + '/file-1', 'more random data', function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'append' }, path + '/file-1')).be.truthy();

      done();
    });
  });

  it('should create and stream data to a file', function (done) {
    var localFileStream = fs.createReadStream(__filename);
    var remoteFileStream = client.createWriteStream(path + '/file-2');
    var spy = sinon.spy();

    localFileStream.pipe(remoteFileStream);
    remoteFileStream.on('error', spy);

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();
      demand(matchLastCallParams({ op: 'create' }, path + '/file-2')).be.truthy();

      done();
    });
  });

  it('should append stream content to an existing file', function (done) {
    var localFileStream = fs.createReadStream(__filename);
    var remoteFileStream = client.createWriteStream(path + '/file-2', true);
    var spy = sinon.spy();

    localFileStream.pipe(remoteFileStream);
    remoteFileStream.on('error', spy);

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();
      demand(matchLastCallParams({ op: 'append' }, path + '/file-2')).be.truthy();

      done();
    });
  });

  it('should open and read a file stream', function (done) {
    var remoteFileStream = client.createReadStream(path + '/file-1');
    var spy = sinon.spy();
    var data = [];

    remoteFileStream.on('error', spy);
    remoteFileStream.on('data', function onData (chunk) {
      data.push(chunk);
    });

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();
      demand(Buffer.concat(data).toString()).be.equal('random datamore random data');
      demand(matchLastCallParams({ op: 'open' }, path + '/file-1')).be.truthy();

      done();
    });
  });

  it('should open and read a file', function (done) {
    client.readFile(path + '/file-1', function (err, data) {
      demand(err).be.null();
      demand(data.toString()).be.equal('random datamore random data');
      demand(matchLastCallParams({ op: 'open' }, path + '/file-1')).be.truthy();

      done();
    });
  });

  it('should list directory status', function (done) {
    client.readdir(path, function (err, files) {
      demand(err).be.null();
      demand(files).have.length(2);

      demand(files[0].pathSuffix).to.eql('file-1');
      demand(files[1].pathSuffix).to.eql('file-2');

      demand(files[0].type).to.eql('FILE');
      demand(files[1].type).to.eql('FILE');

      demand(matchLastCallParams({ op: 'liststatus' }, path)).be.truthy();

      done();
    });
  });

  it('should list directory status', function (done) {
    client.readdir(path + '/dir1', function (err, files) {
      demand(files).length(0);
      demand(matchLastCallParams({ op: 'liststatus' }, path + '/dir1')).be.truthy();

      done();
    });
  });

  it('should change file permissions', function (done) {
    client.chmod(path, '0777', function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'setpermission', permission: '0777' }, path)).be.truthy();

      done();
    });
  });

  it('should return an error when trying to change inexisting file permission', function (done) {
    client.chmod(path + '/file-4', '0777', function (err) {
      demand(err).be.not.null();
      demand(matchLastCallParams({ op: 'setpermission', permission: '0777' }, path + '/file-4')).be.truthy();

      done();
    });
  });

  it('should change file owner', function (done) {
    client.chown(path, process.env.USER, 'supergroup', function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'setowner', owner: process.env.USER, group: 'supergroup' }, path)).be.truthy();

      done();
    });
  });

  it('should return an error when trying to change inexisting file owner', function (done) {
    client.chmod(path + '/file-4', '0777', function (err) {
      demand(err).be.not.null();
      demand(matchLastCallParams({ op: 'setpermission', permission: '0777' }, path + '/file-4')).be.truthy();

      done();
    });
  });

  it('should rename file', function (done) {
    client.rename(path + '/file-2', path + '/bigfile', function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'rename', destination: path + '/bigfile' }, path + '/file-2')).be.truthy();

      done();
    });
  });

  it('should return an error if destination file already exist', function (done) {
    client.rename(path + '/file-1', path + '/bigfile', function (err) {
      demand(err).be.not.null();
      demand(matchLastCallParams({ op: 'rename', destination: path + '/bigfile' }, path + '/file-1')).be.truthy();

      done();
    });
  });

  it('should return an error if destination file path is missing', function (done) {
    client.rename(path + '/file-1', undefined, function (err) {
      demand(err).be.not.null();
      demand(matchLastCallParams({ op: 'rename' }, path + '/file-1')).be.truthy();

      done();
    });
  });

  it('should check file existence', function (done) {
    client.exists(path + '/bigfile', function (exists) {
      demand(exists).be.true();
      demand(matchLastCallParams({ op: 'getfilestatus' }, path + '/bigfile')).be.truthy();

      done();
    });
  });

  it('should return false if file doesn\'t exist', function (done) {
    client.exists(path + '/bigfile2', function (exists) {
      demand(exists).be.falsy();
      demand(matchLastCallParams({ op: 'getfilestatus' }, path + '/bigfile2')).be.truthy();

      done();
    });
  });

  it('should stat file', function (done) {
    client.stat(path + '/bigfile', function (err, stats) {
      demand(err).be.null();
      demand(stats).be.object();

      demand(stats.type).to.eql('FILE');
      demand(stats.owner).to.eql('webuser');

      demand(matchLastCallParams({ op: 'getfilestatus' }, path + '/bigfile')).be.truthy();

      done();
    });
  });

  it('should return an error when trying to stat inexisting file', function (done) {
    client.stat(path + '/bigfile2', function (err, stats) {
      demand(err).be.not.null();
      demand(matchLastCallParams({ op: 'getfilestatus' }, path + '/bigfile2')).be.truthy();

      done();
    });
  });

  it('should create symbolic link', function (done) {
    client.symlink(path + '/bigfile', path + '/biggerfile', function (err) {
      // Pass if server doesn't support symlinks
      if (err && err.message.indexOf('Symlinks not supported') !== -1) {
        done();
      } else {
        demand(err).be.null();
        demand(matchLastCallParams({ op: 'createsymlink', destination: path + '/biggerfile' }, path + '/bigfile')).be.truthy();

        done();
      }
    });
  });

  it('should return an error when creating symbolic link from inexisting path', function (done) {
    client.symlink(path + '/bigfile2', path + '/biggerfile', function (err) {
      demand(err).be.not.null();
      demand(matchLastCallParams({ op: 'createsymlink', destination: path + '/biggerfile' }, path + '/bigfile2')).be.truthy();

      done();
    });
  });

  it('should return an error when creating symbolic link to existing path', function (done) {
    client.symlink(path + '/bigfile', path + '/file-1', function (err) {
      demand(err).be.not.null();
      demand(matchLastCallParams({ op: 'createsymlink', destination: path + '/file-1' }, path + '/bigfile')).be.truthy();

      done();
    });
  });

  it('should delete file', function (done) {
    client.rmdir(path + '/file-1', function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'delete' }, path + '/file-1')).be.truthy();

      done();
    });
  });

  it('should return an error when trying to delete file inexisting file', function (done) {
    client.rmdir(path + '/file-3', function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'delete' }, path + '/file-3')).be.truthy();

      done();
    });
  });

  it('should delete directory recursively', function (done) {
    client.rmdir(path, true, function (err) {
      demand(err).be.null();
      demand(matchLastCallParams({ op: 'delete' }, path)).be.truthy();

      done();
    });
  });
});