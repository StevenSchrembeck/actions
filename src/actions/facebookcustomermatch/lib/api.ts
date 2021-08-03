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
    email = "EMAIL_SHA256",
    phone = "PHONE_SHA256", // as 7705555555 with no spaces, dashes, zeros. add country code if country field is missing
    gender = "GEN_SHA256", // m for male, f for female
    birthYear = "DOBY_SHA256", // YYYY format. i.e. 1900
    birthMonth = "DOBM_SHA256", // MM format. i.e. 01 for january
    birthDay = "DOBD_SHA256", // DD format. i.e. 01
    lastName = "LN_SHA256",
    firstName = "FN_SHA256",
    firstInitial = "FI_SHA256",
    city = "CT_SHA256", //a-z only, lowercase, no punctuation, no whitespace, no special characters
    state = "ST_SHA256", // 2 character ANSI abbreviation code https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code
    zip = "ZIP_SHA256", // in US i.e. 30008, in UK Area/District/Sector format
    country = "COUNTRY_SHA256", // 2 letter codes https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
    madid = "MADID", // all lowercase, keep hyphens 
    externalId = "EXTERN_ID"
}

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
  
        return response && response.data
      }
}