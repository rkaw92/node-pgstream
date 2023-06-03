const streamNameRegExp = /^[a-zA-Z0-9_]{1,50}$/;

export function validStreamName(streamName: string) {
    return streamNameRegExp.test(streamName);
}
