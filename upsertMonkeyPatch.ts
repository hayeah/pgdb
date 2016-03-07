const BaseQueryCompiler = require("knex/lib/query/compiler");

export default function upsertMonkeyPatch(db) {
  // https://github.com/tgriesser/knex/issues/54#issuecomment-183838939

  const QueryBuilder = db.client.QueryBuilder;
  const QueryCompiler = db.client.QueryCompiler;
  // monkey patch to add onConflict

  QueryBuilder.prototype.onConflict = function (columns: string[], updates: any) {
    // throw error if method is not insert
    this._single["onConflictUpdate"] = {
      columns,
      updates,
    }

    return this;
  }

  const __baseInsert = BaseQueryCompiler.prototype.insert;

  QueryCompiler.prototype.insert = function insert() {
    let sql /*:string */ = __baseInsert.call(this);

    if(this.single.onConflictUpdate) {
      const {columns, updates} = this.single.onConflictUpdate;
      sql += ` on conflict (${this.formatter.columnize(columns)}) `

      const doUpdate = Object.keys(updates).sort().map(key => {
        const val = updates[key];
        return this.formatter.wrap(key) + ' = ' + this.formatter.parameter(val)
      }).join(", ");

      sql += ` do update set ${doUpdate}`
    }

    var returning = this.single.returning;
    if(returning) {
      sql += ' returning ' + this.formatter.columnize(returning);
    }

    return {
      sql,
      returning,
    };
  }
}