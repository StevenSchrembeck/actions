import * as gaxios from "gaxios"

const API_BASE_URL = "https://graph.facebook.com/v11.0/"

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
        const namesAndIds = response.data.map((adAccountMetadata: any) => ({name: adAccountMetadata.name, id: adAccountMetadata.id}))
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

    // TODO CREATE CUSTOM AUDIENCE
    // TODO APPEND TO CUSTOM AUDIENCE
    // TODO REPLACE CUSTOM AUDIENCE


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
        })
  
        return response.data
      }
}