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
        "ids_for_business": {
            "data": [
            {
                "id": "106358305032036",
                "app": {
                "link": "https://www.facebook.com/games/?app_id=268147661731047",
                "name": "LookerActionTest",
                "id": "268147661731047"
                }
            }
            ],
            "paging": ...
        },
        "id": "106358305032036"
    }*/
    async getBusinessAccountIds(): Promise<string[]> {
        const response = await this.apiCall("GET", "me?fields=ids_for_business")
        const ids = response["ids_for_business"].data.map((businessMetadata: any) => businessMetadata.id)
        return ids;
    }

    /*
        Sample response:
        {
            "data": [
                {
                "account_id": "4213326242081640",
                "id": "act_4213326242081640"
                },
                {
                "account_id": "114109700789636", <-- side note: this is the one i use for testing
                "id": "act_114109700789636"
                },
                {
                "account_id": "131002649105099",
                "id": "act_131002649105099"
                }
            ],
            "paging": ...
        }
    */
    async getAdAccountsForBusiness(businessId: string): Promise<string[]> {
        const addAcountsForBusinessUrl = `${businessId}/adaccounts`
        const response = await this.apiCall("GET", addAcountsForBusinessUrl)
        const ids = response.data.map((adAccountMetadata: any) => adAccountMetadata.id)
        return ids
    }

    /*
        Sample response:
        {
        "data": [
            {
            "id": "23847792490850535"
            }
        ],
        "paging":...
        }
    */
    async getCustomAudiences(adAccountId: string): Promise<string[]> {
        const customAudienceUrl = `act_${adAccountId}/customaudiences`
        const response = await this.apiCall("GET", customAudienceUrl)
        const ids = response.map((audienceMetadata: any) => audienceMetadata.id)
        return ids
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