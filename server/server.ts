import {Const, QueryableKeys, schema, Table} from "./core/database";

const testSchema = schema({testKey: {type: "str", pointer: true}} as const)




const table = new Table("hello", testSchema, true)