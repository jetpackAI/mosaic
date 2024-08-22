import http from 'node:http';
import path from 'node:path';
import url from 'node:url';
import { v4 as uuidv4 } from 'uuid';

import { WebSocketServer } from 'ws';
import { Cache, cacheKey } from './Cache.js';
import { createBundle, loadBundle } from './load/bundle.js';
import authorization, { buildVerifier } from './authentication.js';

const CACHE_DIR = '.mosaic/cache';
const BUNDLE_DIR = '.mosaic/bundle';

BigInt.prototype.toJSON = function() { return this.toString() }


export async function dataServer(db, {
  cache = true,
  rest = true,
  socket = true,
  port = 3000
} = {}) {
  const queryCache = cache ? new Cache({ dir: CACHE_DIR }) : null;
  const verifier = await buildVerifier();
  const handleQuery = queryHandler(db, queryCache);
  const app = createHTTPServer(handleQuery, rest, verifier,db);
  if (socket) createSocketServer(app, handleQuery, db);
  db.exec('CREATE TABLE TICKETS (ID STRING PRIMARY KEY, USED BOOLEAN);');

  app.listen(port);
  console.log(`Data server running on port ${port}`);
  if (rest) console.log(`  http://localhost:${port}/`);
  if (socket) console.log(`  ws://localhost:${port}/`);
}

function createHTTPServer(handleQuery, rest, verifier, db) {
  return http.createServer(async (req, resp) => {
    const res = httpResponse(resp);
    const isAuthorized = await authorization(req, res, verifier);
    if (!isAuthorized) {
      res.error(res.errorMessage, res.statusCode);
      return;
    }
    if (!rest) {
      res.done();
      return;
    }

    resp.setHeader('Access-Control-Allow-Origin', '*');
    resp.setHeader('Access-Control-Request-Method', '*');
    resp.setHeader('Access-Control-Allow-Methods', 'OPTIONS, POST, GET');
    resp.setHeader('Access-Control-Allow-Headers', '*');
    resp.setHeader('Access-Control-Max-Age', 2592000);

    const path = url.parse(req.url, true).pathname;
    console.log("path")
    console.log(path)
    if (req.method === 'GET' && path === '/login') {
      console.log("coucou")
      const ticketId = uuidv4();
      const query = `INSERT INTO TICKETS VALUES ('${ticketId}', False);`;
      const results = await db.exec(query)
      console.log("results")
      console.log(results)
      const results2 = await db.query(`SELECT * FROM TICKETS;`)
      console.log("results2")
      console.log(results2)
      res.json({ ticketId });
      // res.json(await retrieve(query, sql => db.query(sql)));
      return;
    }

    switch (req.method) {
      case 'OPTIONS':
        res.done();
        break;
      case 'GET':
        handleQuery(res, url.parse(req.url, true).query);
        break;
      case 'POST': {
        const chunks = [];
        req.on('error', err => res.error(err, 500));
        req.on('data', chunk => chunks.push(chunk));
        req.on('end', () => handleQuery(res, Buffer.concat(chunks)));
        break;
      }
      default:
        res.error(`Unsupported HTTP method: ${req.method}`, 400);
    }
  });
}

async function socketAuthorize(protocol, db) {
  if (!protocol) return false;
  const findTicketQuery = `SELECT * FROM TICKETS WHERE ID = '${protocol}';`;
  const ticketsResults = await db.query(findTicketQuery);
  if (ticketsResults.length === 0) {
    return false;
  }
  if (ticketsResults[0].USED) {
    return false;
  }
  if (!ticketsResults[0].USED) {
    const updateTicketQuery = `UPDATE TICKETS SET USED = True WHERE ID = '${protocol}';`;
    await db.exec(updateTicketQuery);
    return true;
  }
  return false;
}

function createSocketServer(server, handleQuery, db) {
  const wss = new WebSocketServer({ server });

  wss.on('connection', async (socket, request) => {
    const res = socketResponse(socket);
    const protocol = request.headers['sec-websocket-protocol'];
    const isAuthorized = await socketAuthorize(protocol, db);
    if (!isAuthorized) {
      res.error('Unauthorized', 401);
      return;
    }
    socket.on('message', data => handleQuery(res, data));
  });
}

export function queryHandler(db, queryCache) {

  // retrieve query result
  async function retrieve(query, get) {
    const { sql, type, persist } = query;
    const key = cacheKey(sql, type);
    let result = queryCache?.get(key);

    if (result) {
      console.log('CACHE HIT');
    } else {
      result = await get(sql);
      if (persist) {
        queryCache?.set(key, result, { persist });
      }
    }

    return result;
  }

  // query request handler
  return async (res, data) => {
    const t0 = performance.now();

    // parse incoming query
    let query;
    try {
      query = JSON.parse(data);
    } catch (err) {
      res.error(err, 400);
      return;
    }

    try {
      const { sql, type = 'json' } = query;
      console.log(`> ${type.toUpperCase()}${sql ? ' ' + sql : ''}`);

      // process query and return result
      switch (type) {
        case 'exec':
          // Execute query with no return value
          await db.exec(sql);
          res.done();
          break;
        case 'arrow':
          // Apache Arrow response format
          res.arrow(await retrieve(query, sql => db.arrowBuffer(sql)));
          break;
        case 'json':
          // JSON response format
          res.json(await retrieve(query, sql => db.query(sql)));
          break;
        case 'create-bundle':
          // Create a named bundle of precomputed resources
          await createBundle(
            db, queryCache, query.queries,
            path.resolve(BUNDLE_DIR, query.name)
          );
          res.done();
          break;
        case 'load-bundle':
          // Load a named bundle of precomputed resources
          await loadBundle(db, queryCache, path.resolve(BUNDLE_DIR, query.name));
          res.done();
          break;
        default:
          res.error(`Unrecognized command: ${type}`, 400);
      }
    } catch (err) {
      res.error(err, 500);
    }

    console.log('REQUEST', (performance.now() - t0).toFixed(1));
  };
}

function httpResponse(res) {
  return {
    arrow(data) {
      res.setHeader('Content-Type', 'application/vnd.apache.arrow.stream');
      res.end(data);
    },
    json(data) {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(data));
    },
    done() {
      res.writeHead(200);
      res.end();
    },
    error(err, code) {
      console.error(err);
      res.writeHead(code);
      res.end();
    }
  }
}

export function socketResponse(ws) {
  const STRING = { binary: false, fin: true };
  const BINARY = { binary: true, fin: true };

  return {
    arrow(data) {
      ws.send(data, BINARY);
    },
    json(data) {
      ws.send(JSON.stringify(data), STRING);
    },
    done() {
      this.json({});
    },
    error(err) {
      console.error(err);
      this.json({ error: String(err) });
    }
  };
}
