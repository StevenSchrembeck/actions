import * as Hub from "../../../hub";

import * as crypto from "crypto"
import * as oboe from "oboe"
import { Readable } from "stream"

import {UserSchema} from "./api"

const BATCH_SIZE = 10000; // Maximum size allowable by Facebook endpoint

interface FieldMapping {
  lookMLFieldName: string,
  fallbackRegex: any,
  shouldHash: boolean,
  facebookAPIName: UserSchema,
}

// TODO move to separate files once ready
export default class FacebookCustomerMatchExecutor {
  private actionRequest: Hub.ActionRequest
  private doHashingBool: boolean = true
  private batchPromises: Promise<void>[] = []
  private batchQueue: any[] = []
  private currentRequest: Promise<any> | undefined
  private isSchemaDetermined = false
  private rowQueue: any[] = []
  private schema: {[s: string]: object} = {}

  constructor(actionRequest: Hub.ActionRequest, doHashingBool: boolean) {
    this.actionRequest = actionRequest
    this.doHashingBool = doHashingBool
  }
  /*
   * If the Looker column label matches the regex, that label will be added to the schema object
   * with its value set to the corresponding output property path given below.
   * Then when subsequent rows come through, we use the schema object keys to get the columns we care about,
   * and put those values into the corresponding output path, as given by the schema object values.
   *
   * Example 1st row: {"User Email Address": "lukeperry@example.com", "US Zipcode": "90210"}
   * Schema object: {"User Email Address": "hashed_email", "US Zipcode": "address_info.postal_code"}
   * Parsed result: [{"hashed_email": "lukeperry@example.com"}, {"address_info": {"postal_code": "90210"}}]
   *                                   ^^^^^^^ Except the email could actually be a hash
   */
  // private regexes = [
  //   [/email/i, "hashed_email"],
  //   [/phone/i, "hashed_phone_number"],
  //   [/first/i, "address_info.hashed_first_name"],
  //   [/last/i, "address_info.hashed_last_name"],
  //   [/city/i, "address_info.city"],
  //   [/state/i, "address_info.state"],
  //   [/country/i, "address_info.country_code"],
  //   [/postal|zip/i, "address_info.postal_code"],
  // ]
  

  private fieldMapping : FieldMapping[] = [
    {
      lookMLFieldName: "Email",
      fallbackRegex: /email/i,
      shouldHash: true,
      facebookAPIName: UserSchema.email
    }
  ]

  private get batchIsReady() {
    return this.rowQueue.length >= BATCH_SIZE
  }

  private get numBatches() {
    return this.batchPromises.length
  }

  private log(level: String, ...rest: String[]) {
    console.log(level + " " + rest.join(" "));
  }


  async run() {
    try {
      // The ActionRequest.prototype.stream() method is going to await the callback we pass
      // and either resolve the result we return here, or reject with an error from anywhere
      await this.actionRequest.stream(async (downloadStream: Readable) => {
        return this.startAsyncParser(downloadStream)
      })
    } catch (errorReport) {
      // TODO: the oboe fail() handler sends an errorReport object, but that might not be the only thing we catch
      this.log("error", "Streaming parse failure:", errorReport.toString())
    }
    await Promise.all(this.batchPromises)
    this.log("info",
      `Streaming upload complete. Sent ${this.numBatches} batches (batch size = ${BATCH_SIZE})`,
    )
  }

  private async startAsyncParser(downloadStream: Readable) {
    return new Promise<void>((resolve, reject) => {
      oboe(downloadStream)
        .node("!.*", (row: any) => {
          if (!this.isSchemaDetermined) {
            this.determineSchema(row)
          }
          this.handleRow(row)
          this.scheduleBatch()
          return oboe.drop
        })
        .done(() => {
          this.scheduleBatch(true)
          resolve()
        })
        .fail(reject)
    })
  }

  // TODO include LookML field tags (if any) in arguments to this function or as part of "row"
  private determineSchema(row: any) {
    for (const columnLabel of Object.keys(row)) {
      for (const mapping of this.fieldMapping) {
        // TODO lookup LookML field tags to see if they match.
        // doing straight regex for now
        const {fallbackRegex} = mapping

        if(columnLabel.match(fallbackRegex)) {
          this.schema[columnLabel] = mapping
        }
      }
    }
    console.log(`Schema is: ` + JSON.stringify(this.schema))
    this.isSchemaDetermined = true
  }

  private handleRow(row: any) {
    const output = this.transformRow(row)
    this.rowQueue.push(...output)
  }


  /* 
    Transforms a row of Looker data into a row of data formatted for the Facebook marketing API.
    Missing data is filled in with empty strings to maintain array order.
  */
  private transformRow(row: any) {
    const schemaMapping = Object.entries(this.schema) as [string, FieldMapping][]
    return schemaMapping.map(( [columnLabel, mapping] ) => {
      let outputValue = row[columnLabel]
      if (!outputValue) {
        return ""
      }
      if (this.doHashingBool && mapping.shouldHash) {
        outputValue = this.normalizeAndHash(outputValue) // TODO separate normalization from hashing
      }
      // TODO do formatting conversion here
      return outputValue
    })
  }

  // TODO Uncomment when needed
  // private getAPIFormattedSchema() {
  //   if(this.schema && this.isSchemaDetermined) {
  //     return Object.values(this.schema).map((fieldMapping) => fieldMapping.facebookAPIName)
  //   }
  //   return null
  // }

  private normalizeAndHash(rawValue: string) {
    const normalized = rawValue.trim().toLowerCase()
    const hashed = crypto.createHash("sha256").update(normalized).digest("hex")
    return hashed
  }

  private scheduleBatch(force = false) {
    if ( !this.batchIsReady && !force ) {
      return
    }
    const batch = this.rowQueue.splice(0, BATCH_SIZE - 1)
    this.batchQueue.push(batch)
    this.batchPromises.push(this.sendBatch())
  }

  private async sendBatch(): Promise<void> {
    if (this.currentRequest !== undefined || this.batchQueue.length === 0) {
      return;
    }
    const currentBatch = this.batchQueue.shift();
    this.currentRequest = new Promise<void>((resolve) => {
      this.log("Pretending to send current batch: ", JSON.stringify(currentBatch));
      resolve();
    });
    await this.currentRequest;
    this.currentRequest = undefined;
    return this.sendBatch();
  }
}