// ORM criticism
// https://web.archive.org/web/20121020042747/http://blog.objectmentor.com/articles/2007/11/02/active-record-vs-objects
// http://programmers.stackexchange.com/questions/119352/does-the-activerecord-pattern-follow-encourage-the-solid-design-principles

// LINQ might be an interesting model to study.

/*

+ upsert?
  + http://stackoverflow.com/questions/17267417/how-do-i-do-an-upsert-merge-insert-on-duplicate-update-in-postgresql
  + http://www.postgresql.org/docs/9.5/static/sql-insert.html
+ non-emuerating uuid?
  + uuid v1 might have better performance characteristics. it's time-based sequence, and as such won't cause table fragmentation.

*/

/*
using iterator to model relationship is pretty interesting...

var q =
   from c in db.Customers
   from o in c.Orders
   where c.City == "London"
   select new { c, o };
*/

const knex = require("knex");

import upsertMonkeyPatch from "./upsertMonkeyPatch";

export interface Course {
  id: number,
  name: string,
  title: string,
  permalink: string,
  free: boolean,
  repo_url: string,
}

export interface CourseInvite {
  id?: number,
  course_id: number,
  token: string,
  created_at: Date,
  updated_at: Date,
  start_time: Date,
}

export let db;


// {
//     client: "pg",
//     connection: config.postgresURL,
//     debug: true,
//   }

interface DBConnectionConfig {
  connection: string;
}

export function connect(config: DBConnectionConfig): void {
  if (db) {
    return;
  }

  console.log("db:", config.connection);

  // TODO: doesn't seem to be a way to know whether the connection succeed?
  db = knex(Object.assign({ client: "pg" }, config));

  upsertMonkeyPatch(db);


  // Same as {debug: true}
  // db.on("query",(obj) => {
  //   console.log("db query", obj);
  // });

  // lame. not what i think it is.
  // return new Promise<any>(resolve => {
  //   db.once("start", () => {
  //     console.log("emit start dammit!");
  //   });
  //   db.once("start", resolve);
  // });
}

export async function disconnect(): Promise<any> {
  if (db === undefined) {
    return;
  }

  var err = await db.destroy();
  db = undefined;
  return err;
}



export type QueryWhereObject = { [key: string]: any }

interface InsertPromise extends Promise<number> {
  onConflict(columns: string[], updates: any): InsertPromise;
  returning(column: string): Promise<any[]>;
}

export interface Query<T> extends Promise<T[]> {
  where(q: QueryWhereObject): this,
  where(key: string, constraint: any): this,
  where(key: string, operator: any, constraint: any): this,
  whereNot(q: QueryWhereObject): this,
  whereRaw(query: string, ...bindings: any[]): this,
  limit(value: number): this,
  offset(value: number): this,
  orderBy(column: string, by: "desc" | "asc"): this,
  select(...columns: string[]): Query<any>,
  first(): Promise<T>,

  join(table: string, key1: string, key2: string): Query<any>,
  select(columns: string[]): Query<any>,

  update(obj: any): InsertPromise,

  // insert(objs: T[]): Promise<number[]>,
  toString(): string,
  toSQL(): string,

  insert(obj: any): InsertPromise;
}

interface TableConfig {
  tableName: string;
  primaryKey?: string;
  createTimestamp?: string;
  updateTimestamp?: string;
}

type ID = number | string;
type Record = { [key: string]: any };

// CRUD
export interface Table<T> {

  where(obj: Record): Query<T>;
  whereNot(obj: Record): Query<T>;

  find(id: ID): Promise<T>;

  // Doesn't work for method.
  // https://github.com/Microsoft/TypeScript/issues/7079
  // insert<T extends U, U>(obj: U): T;
  insert(obj: Record): Promise<T>;
  insertVoid(obj: Record): Promise<void>;

  upsert(obj: Record, upsertUniqueColumns: string[]): Promise<T>;

  update(key: ID, obj: Record): Promise<T>;
  updateVoid(key: ID, obj: Record): Promise<void>;

  // update(id: ID, Record): T;

  // delete(id: ID): boolean;
}

export function Table<T>(config: TableConfig): Table<T> {
  let {tableName,
    primaryKey,
    createTimestamp,
    updateTimestamp,
  } = config;
  if (primaryKey === undefined) {
    primaryKey = "id";
  }

  function timestampCreate(obj) {
    if (createTimestamp === undefined && updateTimestamp === undefined) {
      return obj;
    }

    const now = new Date();
    const timestamps = {};

    if (createTimestamp) {
      timestamps[createTimestamp] = now;
    }

    if (updateTimestamp) {
      timestamps[updateTimestamp] = now;
    }

    return Object.assign(timestamps, obj);
  }


  function timestampUpdate(obj) {
    if (updateTimestamp === undefined) {
      return obj;
    }

    const now = new Date();
    const timestamps = { [updateTimestamp]: now };

    return Object.assign(timestamps, obj);
  }

  class _Table {
    get table(): Query<T> {
      return db(tableName);
    }

    where(obj: Record): Query<T> {
      return this.table.where(obj);
    }

    whereNot(obj: Record): Query<T> {
      return this.table.whereNot(obj);
    }

    async find(id: ID) {
      return this.table.where({ [primaryKey]: id }).first();
    }

    async insertVoid(obj: Record): Promise<void> {
      const insert = this.table.insert(timestampCreate(obj));

      await insert;

      return;
    }

    async insert(obj: Record): Promise<T> {
      const insert = this.table.insert(timestampCreate(obj)).returning("*");
      return (await insert)[0];
    }

    async upsert(obj: Record, upsertUniqueColumns: string[]): Promise<T> {
      const insert = this.table.insert(timestampCreate(obj)).onConflict(upsertUniqueColumns, timestampUpdate(obj)).returning("*");
      return (await insert)[0];
    }

    async update(key: ID, obj: Record): Promise<T> {
      const update = this._update(key, obj).returning("*");

      return (await update)[0];
    }

    async updateVoid(key: ID, obj: Record): Promise<void> {
      await this._update(key, obj);

      return;
    };

    private _update(key: ID, obj: Record) {
      if (key == null) {
        throw new Error("Key cannot be null for update");
      }

      return this.table.where({ [primaryKey]: key }).limit(1).update(obj);
    }
  }
  return new _Table();
}

