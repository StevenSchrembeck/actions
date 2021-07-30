import * as gaxios from "gaxios"

const API_BASE_URL = "https://www.facebook.com/v11.0/"

export default class FacebookCustomerMatchApi {
    readonly accessToken: string
    constructor(accessToken: string) {
        this.accessToken = accessToken
    }

    async me() {
        return this.apiCall("GET", 'me')
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