import * as Hub from "../../hub";

// const LOG_PREFIX = "[FB Ads Customer Match]"

export class FacebookCustomerMatchAction extends Hub.Action {

  readonly name = "facebook_ads_customer_match"
  readonly label = "Facebook Ads Customer Match"
  readonly iconName = "facebook-customer-match/facebook_ads_icon.png"
  readonly description = "TODO."
  readonly supportedActionTypes = [Hub.ActionType.Query]
  readonly supportedFormats = [Hub.ActionFormat.JsonLabel]
  readonly supportedFormattings = [Hub.ActionFormatting.Unformatted]
  readonly supportedVisualizationFormattings = [Hub.ActionVisualizationFormatting.Noapply]
  readonly supportedDownloadSettings = [Hub.ActionDownloadSettings.Url]
  readonly usesStreaming = true
  readonly requiredFields = []
  readonly params = []


  async execute(hubRequest: Hub.ActionRequest) {
    console.log("Begin running")
    const executor = new FacebookCustomerMatchExecutor(hubRequest, true);
    await executor.run()
    console.log("Done running!");
    return new Hub.ActionResponse();
  }

  async form() {
    let form = new Hub.ActionForm()
    form.fields = [{
      label: "Test1",
      name: "test1",
      required: true,
      type: "string",
    }, {
      label: "Test2",
      name: "test2",
      required: true,
      type: "string",
    }]
    return form
  }

}





import * as crypto from "crypto"
import * as lodash from "lodash"
import * as oboe from "oboe"
import { Readable } from "stream"

const BATCH_SIZE = 5000; // Maximum size allowable by Facebook endpoint
// TODO move to separate files once ready
export class FacebookCustomerMatchExecutor {
  private actionRequest: Hub.ActionRequest
  private doHashingBool: boolean = true
  private batchPromises: Promise<void>[] = []
  private batchQueue: any[] = []
  private currentRequest: Promise<any> | undefined
  private isSchemaDetermined = false
  private rowQueue: any[] = []
  private schema: {[s: string]: string} = {}

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
  private regexes = [
    [/email/i, "hashed_email"],
    [/phone/i, "hashed_phone_number"],
    [/first/i, "address_info.hashed_first_name"],
    [/last/i, "address_info.hashed_last_name"],
    [/city/i, "address_info.city"],
    [/state/i, "address_info.state"],
    [/country/i, "address_info.country_code"],
    [/postal|zip/i, "address_info.postal_code"],
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
          debugger;
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

  private determineSchema(row: any) {
    for (const columnLabel of Object.keys(row)) {
      for (const mapping of this.regexes) {
        const [regex, outputPath] = mapping
        if (columnLabel.match(regex)) {
          this.schema[columnLabel] = outputPath as string
        }
      }
    }
    this.isSchemaDetermined = true
  }

  private handleRow(row: any) {
    const output = this.transformRow(row)
    this.rowQueue.push(...output)
  }

  private transformRow(row: any) {
    const schemaMapping = Object.entries(this.schema)
    const outputCells = schemaMapping.map(( [columnLabel, outputPath] ) => {
      let outputValue = row[columnLabel]
      if (!outputValue) {
        return null
      }
      if (this.doHashingBool && outputPath.includes("hashed")) {
        outputValue = this.normalizeAndHash(outputValue)
      }
      return lodash.set({} as any, outputPath, outputValue)
    })
    return outputCells.filter(Boolean)
  }

  // Formatting guidelines: https://support.google.com/google-ads/answer/7476159?hl=en
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

  // The Ads API seems to generate a concurrent modification exception if we have multiple
  // addDataJobOperations requests in progress at one time. So we use this funky solution
  // to run one at a time, without having to refactor the streaming parser and everything too.
  private async sendBatch(): Promise<void> {
    debugger;
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

Hub.addAction(new FacebookCustomerMatchAction())