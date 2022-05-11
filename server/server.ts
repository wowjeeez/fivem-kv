import {schema, Table} from "./core/database";

const testSchema = schema({hello: "bool"})

const table = new Table("hello", testSchema, true)

//table.writeToKey("hello", {testKey: {playerName: "", playerRank: 10, medals: [{type: "val", grantedOn: "str", data: "20"}]}})