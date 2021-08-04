import * as gaxios from "gaxios"
import {sanitizeError} from "./util"

const API_BASE_URL = "https://graph.facebook.com/v11.0/"
export const customer_list_source_types = { // Used by Facebook for unknown purposes. Privacy? Probably not, huh. Currently hardcoded to USER_PROVIDED_ONLY
    USER_PROVIDED_ONLY: "USER_PROVIDED_ONLY",
    PARTNER_PROVIDED_ONLY: "PARTNER_PROVIDED_ONLY",
    BOTH_USER_AND_PARTNER_PROVIDED: "BOTH_USER_AND_PARTNER_PROVIDED"
}
export interface UserUploadSession {
    "session_id": string, 
    "batch_seq":number, 
    "last_batch_flag": boolean, 
    "estimated_num_total"?: number
}

export interface UserUploadPayload {
    "schema": string | string[],
    "data": string[] | string[][],
}

export enum UserSchema { // all lower case all the time
    email = "EMAIL",
    phone = "PHONE", // as 7705555555 with no spaces, dashes, zeros. add country code if country field is missing
    gender = "GEN", // m for male, f for female
    birthYear = "DOBY", // YYYY format. i.e. 1900
    birthMonth = "DOBM", // MM format. i.e. 01 for january
    birthDayOfMonth = "DOBD", // DD format. i.e. 01
    birthday = "DOB", //YYYYMMDD
    lastName = "LN",
    firstName = "FN",
    firstInitial = "FI",
    city = "CT", //a-z only, lowercase, no punctuation, no whitespace, no special characters
    state = "ST", // 2 character ANSI abbreviation code https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code
    zip = "ZIP", // in US i.e. 30008, in UK Area/District/Sector format
    country = "COUNTRY", // 2 letter codes https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
    madid = "MADID", // all lowercase, keep hyphens 
    externalId = "EXTERN_ID"
}

export interface UserFields {
    email?: string | null,
    phone?: string | null,
    gender?: string | null,
    birthYear?: string | null,
    birthMonth?: string | null,
    birthDayOfMonth?: string | null,
    birthday?: string | null,
    lastName?: string | null,
    firstName?: string | null,
    firstInitial?: string | null,
    city?: string | null,
    state?: string | null,
    zip?: string | null,
    country?: string | null,
    madid?: string | null,
    externalId?: string | null,
    [key: string]: UserFields[keyof UserFields]
}

// [transformFunction, the multikey name facebook expects to see]
export const validFacebookHashCombinations: [(f: UserFields) => string, string][] = [
    [(formattedRow: UserFields) => `${formattedRow.email}`, "EMAIL_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.phone}`, "PHONE_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstName}_${formattedRow.city}_${formattedRow.state}`, "LN_FN_CT_ST_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstName}_${formattedRow.zip}`, "LN_FN_ZIP_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.madid}`, "MADID_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.email}_${formattedRow.firstName}`, "EMAIL_FN_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.email}_${formattedRow.lastName}`, "EMAIL_LN_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.phone}_${formattedRow.firstName}`, "PHONE_FN_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstName}_${formattedRow.zip}_${formattedRow.birthYear}`, "LN_FN_ZIP_DOBY_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstName}_${formattedRow.city}_${formattedRow.state}_${formattedRow.birthYear}`, "LN_FN_CT_ST_DOBY_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstInitial}_${formattedRow.zip}`, "LN_FI_ZIP_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstInitial}_${formattedRow.city}_${formattedRow.state}`, "LN_FI_CT_ST_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstInitial}_${formattedRow.state}_${formattedRow.birthday}`, "LN_FI_ST_DOB_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstName}_${formattedRow.state}_${formattedRow.birthYear}`, "LN_FN_ST_DOBY_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstName}_${formattedRow.country}_${formattedRow.birthday}`, "LN_FN_COUNTRY_DOB_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.lastName}_${formattedRow.firstName}_${formattedRow.birthday}`, "LN_FN_DOB_SHA256"],
    [(formattedRow: UserFields) => `${formattedRow.externalId}`, "EXTERN_ID"],
  ]  

export default class FacebookCustomerMatchApi {
    readonly accessToken: string
    constructor(accessToken: string) {
        this.accessToken = accessToken
    }

    async me(): Promise<any> {
        return this.apiCall("GET", "me")
    }

    /*Sample response:
    {
        "businesses": {
            "data": [
            {
                "id": "497949387983810",
                "name": "Webcraft LLC"
            },
            {
                "id": "104000287081747",
                "name": "4 Mile Analytics"
            }
            ],
        }
        "paging": ...
        "id": "106358305032036"
    }*/
    async getBusinessAccountIds(): Promise<{name: string, id: string}[]> {
        const response = await this.apiCall("GET", "me?fields=businesses")
        const namesAndIds = response["businesses"].data.map((businessMetadata: any) => ({name: businessMetadata.name, id: businessMetadata.id}))
        return namesAndIds
    }

    /*
        Sample response:

        {
            "data": [
                {
                "name": "Test Ad Account 1",
                "account_id": "114109700789636",
                "id": "act_114109700789636"
                }
            ],
            "paging": {}
        }
    */
    async getAdAccountsForBusiness(businessId: string): Promise<{name: string, id: string}[]> {
        const addAcountsForBusinessUrl = `${businessId}/owned_ad_accounts?fields=name,account_id`
        const response = await this.apiCall("GET", addAcountsForBusinessUrl)
        const namesAndIds = response.data.map((adAccountMetadata: any) => ({name: adAccountMetadata.name, id: adAccountMetadata.account_id}))
        return namesAndIds
    }

    /*
        Sample response:
        {
        "data": [
            {
            "name": "My new Custom Audience",
            "id": "23847792490850535"
            }
        ],
        "paging":...
        }
    */
    async getCustomAudiences(adAccountId: string): Promise<{name: string, id: string}[]> {
        const customAudienceUrl = `act_${adAccountId}/customaudiences?fields=name`
        const response = await this.apiCall("GET", customAudienceUrl)
        const namesAndIds = response.data.map((customAudienceMetadata: any) => ({name: customAudienceMetadata.name, id: customAudienceMetadata.id}))
        return namesAndIds
    }

    async createCustomAudience(adAccountId: string, name: string, description:string = "", customer_file_source: any = customer_list_source_types.USER_PROVIDED_ONLY) {
        const createCustomAudienceUrl = `act_${adAccountId}/customaudiences`
        const response = await this.apiCall("POST", createCustomAudienceUrl, {
            name,
            description,
            customer_file_source,
            subtype: "CUSTOM"
        })
        return response.data
    }

    async appendUsersToCustomAudience(customAudienceId: string, session: UserUploadSession, payload: UserUploadPayload ) {
        const appendUrl = `${customAudienceId}/users`
        const response = await this.apiCall("POST", appendUrl, {
            session,
            payload,
        })
        return response.data
    }

    async replaceUsersInCustomAudience(customAudienceId: string, session: UserUploadSession, payload: UserUploadPayload ) {
        const appendUrl = `${customAudienceId}/usersreplace`
        const response = await this.apiCall("POST", appendUrl, {
            session,
            payload,
        })
        return response.data
    }


    async apiCall(method: "GET" | "POST", url: string, data?: any) {
        let queryParamCharacter = "?"
        if (url.indexOf("?")) { // don't use two question marks if the url already contains query parameters
            queryParamCharacter = "&"
        }
        const response = await gaxios.request<any>({
          method,
          url: url + `${queryParamCharacter}access_token=${this.accessToken}`,
          data,
          baseURL: API_BASE_URL,
        }).catch((err) => {
            sanitizeError(err)
            // Note that the access token is intentionally omitted from this log
            console.error(`Error in network request ${method} ${url} with parameters: ${typeof data === 'object' && JSON.stringify(data)}. Complete error was: ${err}`)
        })
        if(response && response.data && response.data.error && response.data.error.message) {
            console.log("Facebook error message was: " + response.data.error.message)
        }
  
        return response && response.data
      }
}