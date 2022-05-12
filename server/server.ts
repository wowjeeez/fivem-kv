import {Const, QueryableKeys, schema, Table} from "./core/database";

const testSchema = schema([{value: "str", arrayVal: ["int"]}] as const)




const table = new Table("hello", testSchema, true)


table.writeToKey("hello", [{value: "wow", arrayVal: [10]}])