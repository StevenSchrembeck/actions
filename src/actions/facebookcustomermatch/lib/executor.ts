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
  private batchIncrement: number = 0

  constructor(actionRequest: Hub.ActionRequest, doHashingBool: boolean) {
    this.actionRequest = actionRequest
    this.doHashingBool = doHashingBool
  }

  private fieldMapping : FieldMapping[] = [
    {
      lookMLFieldName: "Email",
      fallbackRegex: /email/i,
      shouldHash: true,
      facebookAPIName: UserSchema.email
    },
    {
      lookMLFieldName: "Phone",
      fallbackRegex: /phone/i,
      shouldHash: true,
      facebookAPIName: UserSchema.phone
    },
    {
      lookMLFieldName: "Gender",
      fallbackRegex: /gender/i,
      shouldHash: true,
      facebookAPIName: UserSchema.gender
    },
    {
      lookMLFieldName: "BirthYear",
      fallbackRegex: /year/i,
      shouldHash: true,
      facebookAPIName: UserSchema.birthYear
    },
    {
      lookMLFieldName: "BirthMonth",
      fallbackRegex: /month/i,
      shouldHash: true,
      facebookAPIName: UserSchema.birthMonth
    },
    {
      lookMLFieldName: "BirthDay",
      fallbackRegex: /day/i,
      shouldHash: true,
      facebookAPIName: UserSchema.birthDay
    },
    {
      lookMLFieldName: "LastName",
      fallbackRegex: /last/i,
      shouldHash: true,
      facebookAPIName: UserSchema.lastName
    },
    {
      lookMLFieldName: "FirstName",
      fallbackRegex: /first/i,
      shouldHash: true,
      facebookAPIName: UserSchema.firstName
    },
    {
      lookMLFieldName: "FirstInitial",
      fallbackRegex: /initial/i,
      shouldHash: true,
      facebookAPIName: UserSchema.firstInitial
    },
    {
      lookMLFieldName: "City",
      fallbackRegex: /city/i,
      shouldHash: true,
      facebookAPIName: UserSchema.city
    },
    {
      lookMLFieldName: "State",
      fallbackRegex: /state/i,
      shouldHash: true,
      facebookAPIName: UserSchema.state
    },
    {
      lookMLFieldName: "Zip",
      fallbackRegex: /postal|zip/i,
      shouldHash: true,
      facebookAPIName: UserSchema.zip
    },
    {
      lookMLFieldName: "Country",
      fallbackRegex: /country/i,
      shouldHash: true,
      facebookAPIName: UserSchema.country
    },
    {
      lookMLFieldName: "MadID",
      fallbackRegex: /madid/i,
      shouldHash: false,
      facebookAPIName: UserSchema.madid
    },
    {
      lookMLFieldName: "ExternalID",
      fallbackRegex: /external/i,
      shouldHash: true,
      facebookAPIName: UserSchema.externalId
    },
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
      outputValue = this.normalize(outputValue)
      if (this.doHashingBool && mapping.shouldHash) {
        outputValue = this.hash(outputValue) // TODO separate normalization from hashing
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

  private hash(rawValue: string) {
    return crypto.createHash("sha256").update(rawValue).digest("hex")
  }
  private normalize(rawValue: string) {
    return rawValue.trim().toLowerCase()
  }

  private scheduleBatch(finalBatch = false) {
    if ( !this.batchIsReady && !finalBatch ) {
      return
    }
    this.batchIncrement += 1
    const batch = {
      data: this.rowQueue.splice(0, BATCH_SIZE - 1),
      batchCount: this.batchIncrement,
      finalBatch
    }
    this.batchQueue.push(batch)
    this.batchPromises.push(this.sendBatch())
  }

  private async sendBatch(): Promise<void> {
    if (this.currentRequest !== undefined || this.batchQueue.length === 0) {
      return;
    }
    const {batchCount, data : currentBatch , finalBatch} = this.batchQueue.shift();
    console.log(batchCount, currentBatch, finalBatch);
    this.currentRequest = new Promise<void>((resolve) => {
      this.log("Pretending to send current batch: ", JSON.stringify(currentBatch));
      resolve();
    });
    await this.currentRequest;
    this.currentRequest = undefined;
    return this.sendBatch();
  }
}