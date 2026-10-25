import http from 'node:http';
import fs from 'node:fs';

// This local audit receiver proves routing without messaging a human destination.
const directory = process.env.ALERT_AUDIT_DIRECTORY ?? '/srv/tidemq/alert-audit';
fs.mkdirSync(directory, {recursive: true, mode: 0o750});
const server = http.createServer((request, response) => {
  if (request.method !== 'POST' || request.url !== '/alerts') {
    response.writeHead(404).end();
    return;
  }
  let body = '';
  request.setEncoding('utf8');
  request.on('data', chunk => {
    body += chunk;
    if (Buffer.byteLength(body) > 1_048_576) request.destroy();
  });
  request.on('end', () => {
    try {
      const event = JSON.parse(body);
      if (!Array.isArray(event.alerts)) throw new Error('Invalid alert payload');
      const file = `${directory}/events.jsonl`;
      if (fs.existsSync(file) && fs.statSync(file).size > 4_194_304) fs.renameSync(file, `${directory}/previous.jsonl`);
      fs.appendFileSync(file, `${JSON.stringify({receivedAt: new Date().toISOString(), ...event})}\n`, {mode: 0o640});
      response.writeHead(200).end('Recorded\n');
    } catch {
      response.writeHead(400).end('Invalid alert payload\n');
    }
  });
});
server.listen(19094, '127.0.0.1');
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => server.close());
