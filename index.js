const r = require('rethinkdbdash')();

class RethinkAdapter {
  constructor(opts = {}) {
    this.opts = Object.assign({}, {
      table: 'shrinku',
    }, opts);
  }

  findByUrl(opts = {}) {
    return r.table(this.opts.table)
      .filter({ url: opts.url })
      .run()
      .then((result) => Promise.resolve(result));
  }

  findByHash(opts = {}) {
    return r.table(this.opts.table)
      .filter({ hash: opts.hash })
      .run()
      .then((result) => Promise.resolve(result));
  }

  find(opts = {}) {
    if (opts.hash) return this.findByHash(opts);
    if (opts.url) return this.findByUrl(opts);

    return Promise.resolve({ url: opts.url, hash: opts.hash });
  }

  save(opts = {}) {
    return r.table(this.opts.table).insert({
      url: opts.url,
      hash: opts.hash,
    }).then((result) => Promise.resolve(result));
  }
}

module.exports = RethinkAdapter;
