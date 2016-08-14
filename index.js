const r = require('rethinkdbdash')();
const Shrinku = require('shrinku');

class RethinkAdapter extends Shrinku.Adapters.BaseAdapter {
  constructor(opts = {}) {
    super();
    this.opts = Object.assign({}, {
      table: 'shrinku',
    }, opts);
  }

  findByUrl(opts = {}) {
    super.findByUrl(opts);
    return r.table(this.opts.table)
      .filter({ url: opts.url })
      .run()
      .then((result) => Promise.resolve((opts.unique ? result[0] : result)));
  }

  findByHash(opts = {}) {
    super.findByHash(opts);
    return r.table(this.opts.table)
      .filter({ hash: opts.hash })
      .run()
      .then((result) => Promise.resolve((opts.unique ? result[0] : result)));
  }

  find(opts = {}) {
    super.find(opts);
    if (opts.hash) return this.findByHash(opts);
    if (opts.url) return this.findByUrl(opts);

    return Promise.reject(new Error('Missing hash or url.'));
  }

  save(opts = {}) {
    return r.table(this.opts.table).insert({
      url: opts.url,
      hash: opts.hash,
    }).then((result) => r.table(this.opts.table).get(result.generated_keys[0]));
  }
}

module.exports = RethinkAdapter;
