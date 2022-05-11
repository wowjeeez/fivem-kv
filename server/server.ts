import {Const, QueryableKeys, schema, Table} from "./core/database";

const testSchema = schema({myKey: {type: "str", pointer: false}, myOtherKey: {type: "str", pointer: false}} as const)
type t = QueryableKeys<typeof testSchema>
let tt: t




const table = new Table("hello", testSchema, true)
table.query(true, {page: 1, limit: 10, queryString: ""})

//table.writeToKey("hello", {testKey: {playerName: "", playerRank: 10, medals: [{type: "val", grantedOn: "str", data: "20"}]}})