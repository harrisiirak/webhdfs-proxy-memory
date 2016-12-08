'use strict';

var p = require('path');
var storage = {};

module.exports = function memoryStorageHandler (err, path, operation, params, req, res, next) {
  // Forward error
  if (err) {
    return next(err);
  }

  switch (operation) {
    case 'mkdirs':
      if (storage.hasOwnProperty(path)) {
        return next(new Error('File already exists'));
      }

      storage[path] = {
        accessTime: (new Date()).getTime(),
        blockSize: 0,
        group: 'supergroup',
        length: 24930,
        modificationTime: (new Date()).getTime(),
        owner: params['user.name'],
        pathSuffix: p.basename(path),
        permission: '644',
        replication: 1,
        type: 'DIRECTORY'
      };
      return next();
      break;

    case 'append':
    case 'create':
      var overwrite = true;
      var exists = storage.hasOwnProperty(path);
      var append = (operation === 'append');

      if (params.hasOwnProperty('overwrite') && !params.overwrite) {
        overwrite = false;
      }

      if (!append && !overwrite && exists) {
        return next(new Error('File already exists'));
      }

      if (!exists) {
        storage[path] = {
          accessTime: (new Date()).getTime(),
          blockSize: 0,
          group: 'supergroup',
          length: 0,
          modificationTime: (new Date()).getTime(),
          owner: params['user.name'],
          pathSuffix: p.basename(path),
          permission: '644',
          replication: 1,
          type: 'FILE',
          data: ''
        };
        var dirn = p.dirname(path);
        if (dirn && dirn !== '.' && !storage.hasOwnProperty(dirn)) {
          storage[dirn] = {
            accessTime: (new Date()).getTime(),
            blockSize: 0,
            group: 'supergroup',
            length: 24930,
            modificationTime: (new Date()).getTime(),
            owner: params['user.name'],
            pathSuffix: p.basename(dirn),
            permission: '644',
            replication: 1,
            type: 'DIRECTORY'
          };
        }
      }

      req.on('data', function onData (data) {
        if (append || storage[path].data.length > 0) {
          storage[path].data += data.toString();
        } else {
          storage[path].data = data.toString();
        }
      });

      req.on('end', function onFinish () {
        storage[path].pathSuffix = p.basename(path);
        storage[path].length = storage[path].data.length;
        return next();
      });

      req.resume();
      break;

    case 'open':
      if (!storage.hasOwnProperty(path)) {
        return next(new Error('File does not exist: ' + path));
      }

      res.writeHead(200, {
        'content-length': storage[path].data.length,
        'content-type': 'application/octet-stream'
      });

      res.end(storage[path].data);
      return next();
      break;

    case 'liststatus':
      var files = [];
      for (var key in storage) {
        if (key !== path && p.dirname(key) === path) {
          files.push(storage[key]);
        }
      }

      var data = JSON.stringify({
        FileStatuses: {
          FileStatus: files
        }
      });

      res.writeHead(200, {
        'content-length': data.length,
        'content-type': 'application/json'
      });

      res.end(data);
      return next();
      break;

    case 'getfilestatus':
      if (!storage.hasOwnProperty(path)) {
        return next(new Error('File does not exist: ' + path));
      }

      var data = JSON.stringify({
        FileStatus: storage[path]
      });

      res.writeHead(200, {
        'content-length': data.length,
        'content-type': 'application/json'
      });

      res.end(data);
      return next();
      break;

    case 'rename':
      if (!storage.hasOwnProperty(path)) {
        return next(new Error('File does not exist: ' + path));
      }

      if (storage.hasOwnProperty(params.destination)) {
        return next(new Error('Destination path exist'));
      }

      storage[params.destination] = storage[path];
      delete storage[path];

      return next();
      break;

    case 'setpermission':
      if (!storage.hasOwnProperty(path)) {
        return next(new Error('File does not exist: ' + path));
      }

      storage[path].permission = params.permission;
      return next();
      break;

    case 'setowner':
      if (!storage.hasOwnProperty(path)) {
        return next(new Error('File does not exist: ' + path));
      }

      storage[path].owner = params.owner;
      storage[path].group = params.group;
      return next();
      break;

    case 'createsymlink':
      if (!storage.hasOwnProperty(path)) {
        return next(new Error('File does not exist: ' + path));
      }

      if (storage.hasOwnProperty(params.destination)) {
        return next(new Error('Destination path exist'));
      }

      storage[params.destination] = storage[path];
      return next();
      break;

    case 'delete':
      if (params.hasOwnProperty('recursive') && params.recursive) {
        var deleted = false;

        for (var key in storage) {
          if (p.dirname(key) === path) {
            delete storage[key];
            deleted = true;
          }
        }

        if (!deleted && !storage.hasOwnProperty(path)) {
          return next(new Error('File does not exist: ' + path));
        }

      } else {
        delete storage[key];
      }

      return next();
      break;

    default:
      return next();
      break;
  }
};
