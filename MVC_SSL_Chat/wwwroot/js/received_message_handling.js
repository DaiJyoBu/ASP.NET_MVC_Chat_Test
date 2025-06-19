"use strict";

// Add the new message to the list of displayed messages.
function AppendReceivedMessage( user, message, time )
{
    // Creating the new html list element that will contain the new message.
    var li = document.createElement("li");

    // Adding the new element into the list.
    document.getElementById("messageList").appendChild(li);

    // Setting the content of the new element.
    li.textContent = `${user} said at ${time}: ${message}`;
}
