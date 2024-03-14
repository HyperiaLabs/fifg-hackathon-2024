import React from 'react';
import axios from "axios";


function useGenericAPICall<T>(url: string) {
    const [data, setData] = React.useState<T | undefined>(undefined);

    React.useEffect(() => {
        get();
    }, []);

    React.useEffect(() => {
        if (data != undefined) {
            setData(undefined);
        }
        get();
    }, [url])

    return {
        data,
        forceReload: get
    };

    async function get() {
        console.log(url);

        const response = await axios.get(url);

        setData(response.data);
    }

}

export default useGenericAPICall;