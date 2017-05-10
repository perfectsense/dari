// No AJAX support?
if (!XMLHttpRequest) {
    setTimeout(function () {
        window.location = RELOADER_WAIT_URL;
    }, 2000);
    
} else {
    
    // Check to see if it's safe to return.
    function check() {
        var req = new XMLHttpRequest();

        req.onreadystatechange = function () {
            if (req.readyState === XMLHttpRequest.DONE) {
                if (req.responseText === 'true') {
                    window.location = RELOADER_RETURN_URL;

                } else {
                    setTimeout(check, 2000);
                }
            }
        };

        req.open('GET', RELOADER_WAIT_URL, true);
        req.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
        req.send(null);
    }

    setTimeout(check, 2000);
}
