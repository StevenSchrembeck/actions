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

    }

    async getCustomAudiences(adAccountId: string) {
        adAccountId = '114109700789636'; // TODO remove hardcoded value
        const customAudienceUrl = `https://graph.facebook.com/v11.0/act_${adAccountId}/customaudiences`
        return await this.apiCall("GET", customAudienceUrl)
    }


    async apiCall(method: "GET" | "POST", url: string, data?: any) {
        const response = await gaxios.request<any>({
          method,
          url: url + `?access_token=${this.accessToken}`,
          data,
          baseURL: API_BASE_URL,
        })
  
        return response.data
      }
}