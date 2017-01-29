/**
 * Created by py on 12/02/16.
 */

function s4() {
    return Math.floor((1 + Math.random()) * 0x10000)
        .toString(16)
        .substring(1);
};

// function that assigns guid to newly created problem
var guid = function () {
    return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
};

module.exports = guid;