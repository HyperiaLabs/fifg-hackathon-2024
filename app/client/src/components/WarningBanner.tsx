import React, { useEffect } from 'react';
import style from '../styles/modules/WarningBanner.module.scss';

const WarningBanner = () => {
    const [show, setShow] = React.useState(true);

    useEffect(() => {
        const timer = setTimeout(() => {
            setShow(false);
        }, 10000);
        return () => clearTimeout(timer);
    }, []);

    return (
        <div className={style['warning-banner'] + ' ' + (show ? style.show : style['is-close'])} title='Click to close' onClick={() => setShow(false)}>
            <p>Please note that this is an ALPHA version, and there may be potential issues. We appreciate your understanding and patience.</p>
            <div className={style.close}></div>
        </div >
    );
};

export default WarningBanner;
