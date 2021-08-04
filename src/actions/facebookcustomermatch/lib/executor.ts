import * as Hub from "../../../hub";

import * as crypto from "crypto"
import * as oboe from "oboe"
import { Readable } from "stream"

import { UserUploadSession, UserUploadPayload, UserFields, validFacebookHashCombinations} from "./api"
import FacebookCustomerMatchApi from "./api"

const BATCH_SIZE = 10000; // Maximum size allowable by Facebook endpoint

interface FieldMapping {
  lookMLFieldName: string,
  fallbackRegex: any,
  userField: string, // The property that ties looker columnLabels to facebook API fields
  normalizationFunction: (s: string) => string // each one is a special snowflake...
}

// TODO move to separate files once ready
export default class FacebookCustomerMatchExecutor {
  private actionRequest: Hub.ActionRequest
  private doHashingBool: boolean = true
  private batchPromises: Promise<void>[] = []
  private batchQueue: any[] = []
  private currentRequest: Promise<any> | undefined
  private isSchemaDetermined = false
  private matchedHashCombinations: [(f: UserFields) => string, string][] = []
  private rowQueue: any[] = []
  private schema: {[s: string]: FieldMapping} = {}
  private batchIncrementer: number = 0
  private sessionId: number
  private facebookAPI: FacebookCustomerMatchApi

  constructor(actionRequest: Hub.ActionRequest, doHashingBool: boolean, accessToken: string) {
    this.actionRequest = actionRequest
    this.doHashingBool = doHashingBool
    this.sessionId =  Date.now() // a unique id used to associate multiple requests with one custom audience API action
    this.facebookAPI = new FacebookCustomerMatchApi(accessToken)
  }

  private fieldMapping : FieldMapping[] = [
    {
      lookMLFieldName: "Email",
      fallbackRegex: /email/i,
      userField: "email",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "Phone",
      fallbackRegex: /phone/i,
      userField: "phone",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "Gender",
      fallbackRegex: /gender/i,
      userField: "gender",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "BirthYear",
      fallbackRegex: /year/i,
      userField: "birthYear",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "BirthMonth",
      fallbackRegex: /month/i,
      userField: "birthMonth",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "BirthDay",
      fallbackRegex: /day/i,
      userField: "birthDay",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "LastName",
      fallbackRegex: /last/i,
      userField: "lastName",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "FirstName",
      fallbackRegex: /first/i,
      userField: "firstName",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "FirstInitial",
      fallbackRegex: /initial/i,
      userField: "firstInitial",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "City",
      fallbackRegex: /city/i,
      userField: "city",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "State",
      fallbackRegex: /state/i,
      userField: "state",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "Zip",
      fallbackRegex: /postal|zip/i,
      userField: "zip",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "Country",
      fallbackRegex: /country/i,
      userField: "country",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "MadID",
      fallbackRegex: /madid/i,
      userField: "madid",
      normalizationFunction: this.normalize
    },
    {
      lookMLFieldName: "ExternalID",
      fallbackRegex: /external/i,
      userField: "externalId",
      normalizationFunction: this.normalize
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
    /* SAMPLE TODO delete
      {
        "choose_business": "497949387983810",
        "choose_ad_account": "114109700789636",
        "choose_create_update_replace": "create_audience",
        "should_hash": "do_no_hashing",
        "choose_custom_audience": "23847998265740535",
        "create_audience_name": "testing1name",
        "create_audience_description": "descriptionhere",
        "format": "json_label"
      }
    */
    console.log("Final form params are: " + JSON.stringify(this.actionRequest.formParams))
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
    const formattedRow = this.getFormattedRow(row, this.schema)
    this.matchedHashCombinations = this.getMatchingHashCombinations(formattedRow, validFacebookHashCombinations)
    this.isSchemaDetermined = true
  }


  /*
IN
    {
      "Users First Name": "Timmy",
      "Users Email": "tt@coolguy.net",
      ...
    },
    {
      "Users First Name": {..., userField: "firstName"},
      ...
    }

OUT
    {
      email: "tt@coolguy.net",
      phone: null,
      gender: null,
      birthYear: null,
      birthMonth: null,
      birthDayOfMonth: null,
      birthday: null,
      lastName: null,
      firstName: "Timmy",
      firstInitial: null,
      city: null,
      state: null,
      zip: null,
      country: null,
      madid: null,
      externalId: null,
    }
*/
  // Get a uniform object that's easy to feed to transform functions
  private getFormattedRow(row: any, schema: {[s: string]: FieldMapping}): UserFields {
    // the first invocation permanently rewrites the function to be much faster by memoizing
    let formattedRow: UserFields = this.getEmptyFormattedRow()
    Object.entries(schema).forEach(([columnLabel, mapping]) => {
      formattedRow[mapping.userField] = row[columnLabel]
    });
    return formattedRow
  }

  private getEmptyFormattedRow(initialValue: string | null | undefined = null) : UserFields {
    return {
      email: initialValue,
      phone: initialValue,
      gender: initialValue,
      birthYear: initialValue,
      birthMonth: initialValue,
      birthDayOfMonth: initialValue,
      birthday: initialValue,
      lastName: initialValue,
      firstName: initialValue,
      firstInitial: initialValue,
      city: initialValue,
      state: initialValue,
      zip: initialValue,
      country: initialValue,
      madid: initialValue,
      externalId: initialValue,
    }
  }

  // Pass in the ones you have and this will return only the hash combinations you have enough data for
  private getMatchingHashCombinations(fieldsWithData: UserFields, hashCombinations: [(f: UserFields) => string, string][]): any[] {
    const dummyFormattedRow = this.getEmptyFormattedRow("EMPTY")
    Object.entries(fieldsWithData).forEach(([field, data]) => {
      if (data !== null) {
        dummyFormattedRow[field] = "FILLED"
      }
    })
    // this was a very fancy way of creating a complete formatted row with only the fields you have using non-null values
  
    // just return the ones that didn't have the EMPTY string in them
    return hashCombinations.filter((hc) => {
      const transformFunction = hc[0]
      const returnedString:string = transformFunction(dummyFormattedRow)
      return returnedString.indexOf("EMPTY") < 0
    })
  }

  private handleRow(row: any) {
    const output = this.transformRow(row)
    this.rowQueue.push(output)
  }

  /* 
    Transforms a row of Looker data into a row of data formatted for the Facebook marketing API.
    Missing data is filled in with empty strings.
  */
  private transformRow(row: any) {
    row = this.normalizeRow(row)    
    const formattedRow = this.getFormattedRow(row, this.schema) // get a uniform object
    // turn our uniform object into X strings like doe_john_30008_1974. One per transform we have enough data for
    let transformedRow = this.matchedHashCombinations.map(([transformFunction, _facebookAPIFieldName]) => {
      if (this.doHashingBool) {
        return this.hash(transformFunction(formattedRow)) || ""
      }
      return transformFunction(formattedRow) || ""
    })
    return transformedRow.length === 1 ? transformedRow[0] : transformedRow; // unwrap an array of one entry, per facebook docs
  }

  private normalizeRow(row: any) {
    const normalizedRow = {...row}
    Object.entries(this.schema).forEach(([columnLabel, mapping]) => {
      normalizedRow[columnLabel] = mapping.normalizationFunction(row[columnLabel])
    })
    return normalizedRow
  }

  private createUploadSessionObject(batchSequence: number, finalBatch: boolean, totalRows?:number): UserUploadSession {
    return {
      "session_id": this.sessionId, 
      "batch_seq":batchSequence, 
      "last_batch_flag": finalBatch, 
      "estimated_num_total": totalRows 
    }
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
      schema: this.matchedHashCombinations.map(([_transformFunction, facebookAPIFieldName]) => facebookAPIFieldName),
      data: currentBatch,
    };

    this.currentRequest = new Promise<void>((resolve) => {
      this.log("Pretending to send current batch: ");
      this.log(JSON.stringify(sessionParameter))
      this.log(JSON.stringify(payloadParameter))
      resolve();
    });

    console.log(this.facebookAPI)
    //                                                   TODO UNHARDCODE \/
    // this.currentRequest = this.facebookAPI.appendUsersToCustomAudience("23847998265740535", sessionParameter, payloadParameter)
    await this.currentRequest;
    this.currentRequest = undefined;
    return this.sendBatch();
  }
}