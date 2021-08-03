import * as Hub from "../../../hub";

import * as crypto from "crypto"
import * as oboe from "oboe"
import { Readable } from "stream"

import {UserSchema, UserUploadSession, UserUploadPayload} from "./api"
import FacebookCustomerMatchApi from "./api"
import { reduceEachLeadingCommentRange } from "typescript";

const BATCH_SIZE = 3; // Maximum size allowable by Facebook endpoint

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
  private schema: {[s: string]: FieldMapping} = {}
  private batchIncrementer: number = 0
  private sessionId: string
  private facebookAPI: FacebookCustomerMatchApi

  constructor(actionRequest: Hub.ActionRequest, doHashingBool: boolean, accessToken: string) {
    this.actionRequest = actionRequest
    this.doHashingBool = doHashingBool
    this.sessionId = "looker_customer_match_" + Date.now() // a unique id used to associate multiple requests with one custom audience API action
    this.facebookAPI = new FacebookCustomerMatchApi(accessToken)
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
    const schemaMapping = Object.entries(this.schema)
    const isSingleColumn = Object.values(this.schema).length === 1
    let transformedRow = schemaMapping.map(( [columnLabel, mapping] ) => {
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
    transformedRow = isSingleColumn ? transformedRow : [transformedRow]; // unwrap an array of one entry, per facebook docs
    return transformedRow
  }

  private createUploadSessionObject(batchSequence: number, finalBatch: boolean, totalRows?:number): UserUploadSession {
    return {
      "session_id": this.sessionId, 
      "batch_seq":batchSequence, 
      "last_batch_flag": finalBatch, 
      "estimated_num_total": totalRows 
    }
  }
  // TODO Uncomment when needed
  private getAPIFormattedSchema(): string | string[] {
    const fieldMapping: FieldMapping[] = Object.values(this.schema)
    const formattedSchema = fieldMapping.map((fieldMapping) => fieldMapping.facebookAPIName)
    return (Object.values(this.schema).length === 1) ? formattedSchema[0] : formattedSchema // unwrap an array of one entry, per facebook docs
  }

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
    this.batchIncrementer += 1
    const batch = {
      data: this.rowQueue.splice(0, BATCH_SIZE - 1),
      batchSequence: this.batchIncrementer,
      finalBatch
    }
    this.batchQueue.push(batch)
    this.batchPromises.push(this.sendBatch())
  }

  private async sendBatch(): Promise<void> {
    if (this.currentRequest !== undefined || this.batchQueue.length === 0) {
      return;
    }
    const {batchSequence, data : currentBatch , finalBatch} = this.batchQueue.shift();
    const sessionParameter = this.createUploadSessionObject(batchSequence, finalBatch)
    const payloadParameter: UserUploadPayload = {
      schema: this.getAPIFormattedSchema(),
      data: currentBatch,
    };

    // this.currentRequest = new Promise<void>((resolve) => {
    //   this.log("Pretending to send current batch: ");
    //   this.log(JSON.stringify(sessionParameter))
    //   this.log(JSON.stringify(payloadParameter))
    //   resolve();
    // });

    //                                                   TODO UNHARDCODE \/
    this.currentRequest = this.facebookAPI.appendUsersToCustomAudience("23847998265740535", sessionParameter, payloadParameter)
    await this.currentRequest;
    this.currentRequest = undefined;
    return this.sendBatch();
  }
}