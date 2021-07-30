import * as gaxios from "gaxios"

const API_BASE_URL = "https://www.facebook.com/v11.0/"

export default class FacebookCustomerMatchApi {
    readonly accessToken: string
    constructor(accessToken: string) {
        this.accessToken = accessToken
    }

    async me() {
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
            "paging": {
                "cursors": {
                    "before": "QVFIUk92XzMyQTQzZAXpVai12WUNJRWR3SWg3VnhvQUhSQkFDbEs4WmlnaTB2TTlKbHdubUZAYNkhDX0p0ckdXUy1fXzNXNnFaRjVkMzlReWIyY0pYeS1qc0hR",
                    "after": "QVFIUk92XzMyQTQzZAXpVai12WUNJRWR3SWg3VnhvQUhSQkFDbEs4WmlnaTB2TTlKbHdubUZAYNkhDX0p0ckdXUy1fXzNXNnFaRjVkMzlReWIyY0pYeS1qc0hR"
                }
            }
        },
        "id": "106358305032036"
    }*/
    async getBusinessAccountIds() {
        return this.apiCall("GET", "me?fields=ids_for_business")
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
            "paging": {
                "cursors": {
                "before": "MjM4NDc1NTgzMzk2MjAyMDEZD",
                "after": "MjM4NDgxNTQwMDI3NTAwODAZD"
                }
            }
        }
    */
    async getAdAccountsForBusiness(businessId: string) {
        const addAcountsForBusinessUrl = `${businessId}/adaccounts`
        return await this.apiCall("GET", addAcountsForBusinessUrl)
    }

    async getCustomAudiences(adAccountId: string) {
        const customAudienceUrl = `act_${adAccountId}/customaudiences`
        return await this.apiCall("GET", customAudienceUrl)
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