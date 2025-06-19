document.addEventListener("DOMContentLoaded", function () {
    const eventSource = new EventSource("/chat/stream");

    // The handler of the server update - the user has sent the message into the chat.
    eventSource.onmessage = function (event)
    {
        // Convert the JSON string into the javascript object.
        const message = JSON.parse(event.data);

        // Display the converted message into the list.
        AppendReceivedMessage( message.AuthorName, message.Content, new Date( message.CreatedTime ).toLocaleString() );
    };
    
    // The handler of the error from the stream.
    eventSource.onerror = function (err) {
        // Send the error data into the console.
        console.error("SSE connection error", err);
        // TBD: auto-reconnect logic
    };
});

// The URI of the controller - 'chat' - and the post method - 'send'
const uri = 'chat/send';
const messageInputElementName = "messageInput";
const sendButtonElementName = "sendButton";

document.getElementById( sendButtonElementName ).addEventListener("click",
    function (event)
    {
        // Obtain the message input and wrap it into the object.
        var message = document.getElementById( messageInputElementName ).value;
        var item = {
            Content: message
        };

        // Send the data wrapped message to server.
        fetch(uri,
            {
                method: 'POST',
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(item)
            }
        ).then( () =>
        {
            // Clear the message input field.
            document.getElementById( messageInputElementName ).value = '';
        }
        ).catch(
            // Send the error data into the console.
            error => console.error('Unable to send message.', error));

        event.preventDefault();
    }
);