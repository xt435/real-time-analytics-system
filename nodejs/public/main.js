// what is IEFI, not sure about this name
$(function () {
    var smoothie = new SmoothieChart({
        timestampFormatter: SmoothieChart.timeFormatter
    });

    smoothie.streamTo(document.getElementById("stockchart"), 1000)

    var line1 = new TimeSeries()
    smoothie.addTimeSeries(line1, {
      lineWidth: 3
    });

    var socket = io();
    // - when receive 'data' event, do something
    socket.on('data', function (data) {
        // $("#data").html(data)
        parsed = JSON.parse(data)
        line1.append(Math.trunc(parsed['timestamp'] * 1000), parsed['average'])
    });
});
