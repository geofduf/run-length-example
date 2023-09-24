function queryRange(h) {
    const match = h.match(/^\d+h|\d+d|[cfp][dwm]|[cf]y$/);
    if (match === null) {
        throw new Error("parameter is not supported");
    }
    const x = h.slice(0, -1);
    const y = h.slice(-1);
    const start = new Date();
    let end = new Date(start);
    if (x === "c" || x === "f" || x === "p") {
        start.setHours(0, 0, 0, 0);
        if (y === "d") {
            if (x === "p") {
                start.setDate(start.getDate()-1);
            }
            if (x !== "f") {
                end = new Date(start);
                end.setHours(23, 59, 59, 0);
            }
        } else if (y === "w") {
            let d = start.getDay() - 1;
            if (d === -1) {
                d = 6;
            }
            if (x === "p") {
                d += 7;
            }
            start.setDate(start.getDate()-d);
            if (x !== "f") {
                end = new Date(start);
                end.setDate(end.getDate()+6);
                end.setHours(23, 59, 59, 0);
            }
        } else if (y === "m") {
            start.setDate(1);
            if (x === "p") {
                start.setMonth(start.getMonth()-1);
            }
            if (x !== "f") {
                end = new Date(start);
                end.setMonth(end.getMonth()+1);
                end.setDate(0);
                end.setHours(23, 59, 59, 0);
            }
        } else if (y === "y") {
            start.setDate(1);
            start.setMonth(0);
            if (x !== "f") {
                end.setDate(31);
                end.setMonth(11);
                end.setHours(23, 59, 59, 0);
            }
        }
    } else if (y === "h") {
        start.setHours(start.getHours()-parseInt(x));
        start.setSeconds(start.getSeconds()+1, 0);
    } else {
      start.setDate(start.getDate()-parseInt(x));
      start.setSeconds(start.getSeconds()+1, 0);
    }
    return [start, end];
}

function pad(x) {
  return x.toString().padStart(2, "0");
}

function dateTimeFormat(x) {
    return `${x.getFullYear()}-${pad(x.getMonth()+1)}-${pad(x.getDate())} ` +
      `${pad(x.getHours())}:${pad(x.getMinutes())}:${pad(x.getSeconds())}`;
}

function toUnixTime(x) {
  return Math.floor(x.getTime()/1000);
}

function refresh(data) {
  for (const e of document.querySelectorAll(".content")) {
    if (data.hasOwnProperty(e.id)) {
      if (e.classList.contains("content-text")) {
        e.innerText = data[e.id];
      } else {
        e.innerHTML = data[e.id];
      }
    } else {
      e.innerHTML = "";
    }
  }
}

function eventHandlerQuery() {
  const key = document.getElementById("key").value;
  if (key.match(/^[a-zA-Z0-9_]+$/) === null) {
    refresh({"content-request": "Please enter a valid key..."});
    return;
  }
  let start, end;
  try {
    [start, end] = queryRange(document.getElementById("history").value);
  } catch (error) {
    refresh({"content-request": "Something weird happened..."});
    return;
  }
  const content = {};
  const url = `/query/?key=${key}&start=${toUnixTime(start)}&end=${toUnixTime(end)}`;
  content["content-request"] = `Key:   ${key}\n` +
    `Start: ${dateTimeFormat(start)}\n` +
    `End:   ${dateTimeFormat(end)}\n` +
    `Raw:   GET ${url}`;
  fetch(url)
    .then(function(response) {
      content["content-response-code"] = response.status;
      return response.json();
    })
    .then(function(body) {
      if (body.hasOwnProperty("status") && body.status === "ok") {
        let table = "<table><thead><tr><th>Time</th><th>Count</th><th>Mean</th></tr></thead><tbody>";
        for (const row of body.data.reverse()) {
          const d = new Date(row.date*1000);
          table += `<tr><td>${dateTimeFormat(d)}</td><td>${row.count}</td><td>${row.mean}</td></tr>`;
        }
        table += "</tbody></table>"
        content["content-rows"] = table;
      }
      content["content-response-body"] = JSON.stringify(body, (k, v) => k === "data" ? undefined : v, 2);
      refresh(content);
    })
    .catch(function(error) {
      content["content-request"] = error;
      refresh(content);
    });
}

function eventHandlerInsert(e) {
  const key = document.getElementById("key").value;
  if (key.match(/^[a-zA-Z0-9_]+$/) === null) {
    refresh({"content-request": "Please enter a valid key..."});
    return;
  }
  let x;
  if (e.target.id === "insert-zero") {
    x = 0;
  } else if (e.target.id === "insert-one") {
    x = 1;
  } else {
    refresh({"content-request": "something weird happened..."});
    return;
  }
  const content = {};
  const body = `${key} ${x}`;
  const url = "/insert/";
  content["content-request"] = `Key:   ${key}\n` +
    `Value: ${x}\n` +
    `Raw:   POST ${url}\n` +
    `Body:  ${body}`;
  fetch(url, {method: "POST", body: body})
    .then(function(response) {
      content["content-response-code"] = response.status;
      return response.json();
    })
    .then(function(body) {
      content["content-response-body"] = JSON.stringify(body, null, 2);
      refresh(content);
    })
    .catch(function(error) {
      content["content-request"] = error;
      refresh(content);
    });
}