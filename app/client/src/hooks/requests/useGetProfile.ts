import useGenericAPICall from "./useGenericAPICall";
import config from "dotenv"


export type ErrorCode = 1;

interface Output {
    profile: any;
    error_code: undefined | ErrorCode
}

export default function useGetProfile(address: string): Output | undefined {
    const url = (process.env.REACT_APP_API_URL ?? "http://127.0.0.1:8000") + "/profile/" + address;
    const { data } = useGenericAPICall<any>(url);

    if (data != undefined) {
        return data;
    }
    else {
        return undefined;
    }
}