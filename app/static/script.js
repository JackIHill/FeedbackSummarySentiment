let expanded = false;
let show = false;

let pendingSelectedDates = []
let pendingDatePreselectButton = null
let submittedDates = []
let submittedDatePreselectButton = null
let submittedOperators = []
let submittedPhrases = []

let initialSelections = [];
let selectionSaved = false
let defFilterItemWidth;
let defFilterItemHeight;

currentlyProcessing = true;

let activePageIndex;
function updatePageIndicator(index, animate = true) {
  const navPages = document.querySelectorAll(".nav-item");
  const pageIndicator = document.querySelector(".nav-indicator");
  const currentNavPage = navPages[index];

  // for styling current active page text
  navPages.forEach((currentNavPage, i) => {
    currentNavPage.classList.toggle('active', i === index);
  });

  const textWidth = currentNavPage.scrollWidth;
  const indicatorPctOfContainerWidth = 0.5;

  const indicatorWidth = textWidth * indicatorPctOfContainerWidth;
  const itemCenter = currentNavPage.offsetLeft + (currentNavPage.offsetWidth / 2);
  const indicatorLeft = itemCenter - (indicatorWidth / 2);

  pageIndicator.style.transition = animate ? 'left 0.3s ease-in-out' : 'none';
  pageIndicator.style.width = `${indicatorWidth}px`;
  pageIndicator.style.left = `${indicatorLeft}px`;

  activePageIndex = index;
}

function goToPage(index) {
  const main = document.getElementById('main-slider');
  main.style.transform = `translateX(-${index * 100}vw)`;
  updatePageIndicator(index)
}

// Pill shown when not on main processing page and processing is active
function showActiveProcessingPill(show) {
  const activePill = document.querySelector('.active-processing-pill')
  let isMouseDown = false;

  if (show && currentlyProcessing) {
    activePill.classList.add('active')

    // clicked class for responsive click (add shrink)
    activePill.addEventListener('mousedown', function () {
      activePill.classList.add('clicked');
      isMouseDown = true;
  });

    document.addEventListener('mouseup', function (event) {
      if (isMouseDown) {
          if (activePill.contains(event.target)) {
              // If insta click unclick, play full shrink transition.
              setTimeout(() => {
                  activePill.classList.remove('clicked');
                  activePill.classList.remove('active');
                  isMouseDown = false;
              }, 200);

              goToPage(0)

          } else {
              // If mouseup outside pill, remove shrink
              activePill.classList.remove('clicked');
              isMouseDown = false;
            }
          }
        });
      }
  else {
    activePill.classList.remove('active')
  }  
}


function dark() {
  document.body.classList.toggle("dark");

  const theme_svg = document.querySelector(".theme-icon");
  const dLight = "M12 7c-2.76 0-5 2.24-5 5s2.24 5 5 5 5-2.24 5-5-2.24-5-5-5M2 13h2c.55 0 1-.45 1-1s-.45-1-1-1H2c-.55 0-1 .45-1 1s.45 1 1 1m18 0h2c.55 0 1-.45 1-1s-.45-1-1-1h-2c-.55 0-1 .45-1 1s.45 1 1 1M11 2v2c0 .55.45 1 1 1s1-.45 1-1V2c0-.55-.45-1-1-1s-1 .45-1 1m0 18v2c0 .55.45 1 1 1s1-.45 1-1v-2c0-.55-.45-1-1-1s-1 .45-1 1M5.99 4.58c-.39-.39-1.03-.39-1.41 0-.39.39-.39 1.03 0 1.41l1.06 1.06c.39.39 1.03.39 1.41 0s.39-1.03 0-1.41zm12.37 12.37c-.39-.39-1.03-.39-1.41 0-.39.39-.39 1.03 0 1.41l1.06 1.06c.39.39 1.03.39 1.41 0 .39-.39.39-1.03 0-1.41zm1.06-10.96c.39-.39.39-1.03 0-1.41-.39-.39-1.03-.39-1.41 0l-1.06 1.06c-.39.39-.39 1.03 0 1.41s1.03.39 1.41 0zM7.05 18.36c.39-.39.39-1.03 0-1.41-.39-.39-1.03-.39-1.41 0l-1.06 1.06c-.39.39-.39 1.03 0 1.41s1.03.39 1.41 0z";
  const dDark = "M9 2c-1.05 0-2.05.16-3 .46 4.06 1.27 7 5.06 7 9.54s-2.94 8.27-7 9.54c.95.3 1.95.46 3 .46 5.52 0 10-4.48 10-10S14.52 2 9 2";

  let path = theme_svg.querySelector(".icon-path");
  let isDarkMode = document.body.classList.contains("dark");

  if (isDarkMode) {
    path.classList.add("rotated");
    path.setAttribute("d", dLight);
  } else {
    path.setAttribute("d", dDark);
    path.classList.remove("rotated");
  }

  // Updates all charts' theme after theme toggle
  Object.keys(window).forEach(chartId => {
    const chart = window[chartId];
    if (chart instanceof Chart) {
      updateChartTheme(chart);
    }
  });


  const sentimentStatElements = document.querySelectorAll('[data-sentiment-value]');
  
  sentimentStatElements.forEach(element => {
    const value = parseFloat(element.dataset.sentimentValue);
    element.style.color = assignSentimentAnHSL(value);
  });


}

// Helper function to get CSS variables
function getCSSVar(variable) {
  return getComputedStyle(document.body).getPropertyValue('--' + variable).trim();
}



// Function to update the chart theme based on the dark mode
function updateChartTheme(chart) {
  const primaryColor = getCSSVar('primary-color');
  const primaryTextColor = getCSSVar('primary-text-color')
  const tertiaryColor = getCSSVar('tertiary-color');
  const accentColor = getCSSVar('accent-color');

  if (!chart.options.scales.y || !chart.options.scales.x) {
    chart.options.plugins.datalabels.color = (context) => {
      const label = context.chart.data.labels[context.dataIndex];
      return assignSentimentAnHSL(label);
    };

    if (chart.data.datasets) {
      chart.data.datasets[0].backgroundColor = chart.data.datasets[0].data.map((value, index) => {
        const sentimentScore = chart.data.labels[index];
        return assignSentimentAnHSL(sentimentScore);
      });
    
      chart.update();
    
    }
    

    return

  }

  chart.options.scales.x.ticks.color = primaryTextColor;
  chart.options.scales.y.ticks.color = primaryTextColor;

  // drawZeroLine plugin
  chart.options.plugins.drawZeroLine.color = accentColor
  chart.options.scales.y.border.color = accentColor

  chart.options.scales.x.grid.color = 'transparent'
  chart.options.scales.y.grid.color = 'transparent'

  chart.options.scales.x.title.color = tertiaryColor
  chart.options.scales.y.title.color = tertiaryColor



  // Update chart bars (background color for dataset)
  chart.data.datasets.forEach(ds => {
    const chartJSDefaultColor = 'rgba(54, 162, 235, 0.5)'
    if (!ds.backgroundColor || ds.backgroundColor === chartJSDefaultColor) {
      ds.backgroundColor = accentColor;
    }
    if (!ds.borderColor || ds.borderColor === chartJSDefaultColor) {
      ds.borderColor = 'transparent';
    }
    });

  chart.update();
}


// 
// Input box filters drop-down
//
function filterSelections(event) {
  const container = event.target.closest(".select-container");
  const input = container.querySelector(".select-text-input").value.toLowerCase();
  const labels = container.querySelectorAll(".drop-down-items .selection-row-wrapper");

  if (input === "") {
    labels.forEach(row => {
      row.style.display = 'inline-flex';
    });
  }
  else {
    labels.forEach(row => {
      const labelText = row.querySelector(".checkbox-label-text").innerText.toLowerCase();
      if (labelText.includes(input)) {
        row.style.display = "inline-flex";
      } else {
        row.style.display = "none"; 
      }
    });
  }
}

document.addEventListener("DOMContentLoaded", function () {
  goToPage(0);

  // set as default for now - later remember user selection.
  dark()

  function saveInitialSelections(container) {
    const checked = container.querySelectorAll(".checkbox-input");
    initialSelections = Array.from(checked).map(cb => cb.checked);

    if (container.id === 'select-container-date-range') {
      if (pendingSelectedDates.length) {
        initialSelections = pendingSelectedDates
      }
    } 
  }

  function restoreInitialSelections(container) {
    const checked = container.querySelectorAll(".checkbox-input");
    selectionSaved = false

    checked.forEach((cb, index) => {
        cb.checked = initialSelections[index];
    });

    if (container.id === 'select-container-date-range') {
      if (pendingSelectedDates.length) {
        datepicker.setDate(initialSelections, true); 
        
        // if a preselect button has been submitted (i.e. 6 months selected, then return to that button on cancel)
        if (submittedDatePreselectButton) {
          activateButtonPersistence(submittedDatePreselectButton)
        } 
        // otherwise, if there was no selected button, remove all persistent buttons (i.e. revert to standard view)
        else if (pendingDatePreselectButton) {
          deactivateButtonPersistence(pendingDatePreselectButton)
        } 
      }
    }
  }


  var yearStart = 100;
  var yearEnd = 2;
  
  var minDate = "2023-11-01";

  // default maxDate is the currentDate. 
  // https://stackoverflow.com/questions/12413243/javascript-date-format-like-iso-but-local/51643788#51643788
  maxDate = new Date().toLocaleString('sv').replace(' ', 'T');
  
  const input = document.getElementById("datepicker");
  // Initialize Flatpickr
  const datepicker = flatpickr(input, {
    plugins: [
      new window.monthDropdownPlugin(),
      new window.yearDropdownPlugin({
        yearStart: yearStart,
        yearEnd: yearEnd
      }),
      new window.updateSelectedDateRangePlugin()
    ],
    locale: {
      weekdays: {
        shorthand: ["S", "M", "T", "W", "T", "F", "S"],
        longhand: ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
      },
    },
    mode: "range",
    dateFormat: "Y-m-d",
    minDate: minDate,
    maxDate: maxDate,
    appendTo: document.querySelector("#drop-down-body-date"),
    // onChange triggers when a range is selected or changed
    onChange: function(selectedDates) {

      // isManualSelection is set in updateSelectedDateRangePlugin().
      // if a manual selection (i.e. not using preselect buttons), then clear the preselect buttons. 
      // TODO: ensure that if the user manually selects the same range as a preselect button, that the corresponding button is persisted.
      if (isManualSelection && pendingDatePreselectButton) {
        deactivateButtonPersistence(pendingDatePreselectButton)
      }

      pendingSelectedDates = [...selectedDates];
      
      updateOkButtonState()
      
    },
    onClose: function(selectedDates, dateStr, instance) {
      instance.open(); // Immediately re-open to prevent default closing on range selection
  }
  
  });



  document.querySelectorAll(".select-container").forEach(selectContainer => {
    const selectContainerDefStyle = window.getComputedStyle(selectContainer);

    // Store initial dimensions per dropdown (used in toggleDropdown() for reseting to default values)
    selectContainer.dataset.defFilterItemWidth = Math.round(parseFloat(selectContainerDefStyle.width));
    selectContainer.dataset.defFilterItemHeight = Math.round(parseFloat(selectContainerDefStyle.height));

  });

  function toggleDropdown(show, event = null) {    

    // Maintain filter container height
    const filterContainers = document.querySelectorAll(".global-filter-container");
    const selectContainer = event?.target?.closest(".select-container");

    if (!selectContainer) return;
    if (!event) return;

    const defFilterItemWidth = selectContainer.dataset.defFilterItemWidth;
    const defFilterItemHeight = selectContainer.dataset.defFilterItemHeight;

    filterContainers.forEach(filterContainer => {
      filterContainer.style.setProperty("--initialFilterContainerHeight", `${filterContainer.offsetHeight}px`)
    });

    const dropDownContainer = selectContainer.querySelector(".drop-down-container");
    const buttonContainers = selectContainer.querySelectorAll(".button-container");
    const arrowIcon = selectContainer.querySelector(".arrow");
    const selectBox = selectContainer.querySelector(".select-box");
    const selectInput = selectContainer.querySelector(".select-text-input");
    const selectOverlay = selectContainer.querySelector(".select-overlay");
    const dropDownBody = selectContainer.querySelector(".drop-down-body");

    const summarySection = document.querySelector("#summary-section");

    const expanded = selectContainer.dataset.expanded === 'true'; 


    // readonly drop downs (can't input text into container) can be toggled on/off by clicking on the container
    // (a text-input container will only open on click; see below)
    const isReadonly = selectInput.classList.contains('readonly')
    if (isReadonly) {
      show = !expanded; 
    }

    // If the dropdown is already open, only close if clicking the select-overlay (dropdown arrow container)
    if (expanded && show && !isReadonly) {
      if (!selectOverlay.contains(event.target)) {
        return;
      } else {
        show = false;
      }
    }
    

    const dropDownElements = [dropDownContainer, arrowIcon, selectBox, selectOverlay, dropDownBody, selectContainer].filter(Boolean);

    const method = show ? "add" : "remove";
    dropDownElements.forEach(element => element.classList[method]("active"));
    buttonContainers.forEach(element => element.classList[method]("active"));


    // summary unaffected by date range... for now.
    if (selectContainer.id === "select-container-date-range") {
      summarySection.classList[method]("disabled");
    }

    // Reset dimensions and transitions
    if (show) {
      dropDownBody.scrollLeft = 0;

      selectContainer.style.width = `${defFilterItemWidth}px`;

      if (selectContainer.id !== "select-container-date-range") {
        // date picker doesn't have resizing.
        selectContainer.style.minWidth = `${defFilterItemWidth * 0.8}px`;
        selectContainer.style.maxWidth = `${defFilterItemWidth * 2}px`;

        selectContainer.style.height = `${defFilterItemHeight * 8}px`; // Approx 8 rows
        selectContainer.style.minHeight = `${defFilterItemHeight * 3}px`;
        selectContainer.style.maxHeight = `${defFilterItemHeight * 12}px`;
        selectContainer.style.transition = "none";

        // Ensure dropdown height updates dynamically with selectContainer (i.e. after/during resize)
        const updateDropdownHeight = () => {
          // don't apply to inner drop downs (not resizable / don't have border.)
            if (dropDownContainer.closest('.inner-drop-down')) {
              return;
            }

            const selectBoxHeight = Math.round(parseFloat(window.getComputedStyle(selectBox).height));
          
            let totalPadding = 0;
            const dropDownChildElements = dropDownContainer.children;
            const selectContainerPadding = Math.round(parseFloat(window.getComputedStyle(selectContainer).paddingBottom))

            Array.from(dropDownChildElements).forEach(childElement => {
              const childElementStyles = Math.round(window.getComputedStyle(childElement));
              totalPadding += parseFloat(childElementStyles.paddingBottom);
            });
          
            const availableHeight = dropDownContainer.parentElement.clientHeight 
                                  - selectBoxHeight 
                                  // - totalPadding
                                  - selectContainerPadding;
          
            dropDownContainer.style.height = `${availableHeight}px`;
            dropDownContainer.style.maxHeight = `${availableHeight}px`;
        };

        updateDropdownHeight();

        // Listen for resize events on selectContainer and update dropdown height
        new ResizeObserver(updateDropdownHeight).observe(selectContainer);


      }
      else {
        datepicker.open(); // Manually trigger calendar open
      }


      if (!expanded) {
        saveInitialSelections(selectContainer);
      }
      selectContainer.dataset.expanded = "true"; // Sets expanded state to true for selected dropdown


    } else {
      // Reset input and dropdown size
      // not applicable to date container as static width/height.
      if (selectContainer.id !== "select-container-date-range") {
      
        selectContainer.style.height = "";
        // selectContainer.style.width = `${defFilterItemWidth}px`; // Allows smooth width transition when returning to default selectbox width.
        selectContainer.style.minHeight = "";
        selectContainer.style.transition = "width 0.2s ease";

        dropDownContainer.style.height = "";
        dropDownContainer.style.maxHeight = "";

        filterSelections(event);
      }

      selectContainer.dataset.expanded = "false"; // Sets expanded state to false for selected dropdown
      datepicker.close()

    }
    updateOkButtonState()

  }

  // make accessible to plugin 
  window.toggleDropdown = toggleDropdown;



  // disable ok button if no checkboxes selected
  function updateOkButtonState() {
    document.querySelectorAll(".select-container").forEach(selectContainer => {
      const okButton = selectContainer.querySelector(".ok-button");
      const checkboxes = selectContainer.querySelectorAll("input[name^='selection-']");
  
      let disable = false;
  
      if (checkboxes.length > 0) {
        const selectedCheckboxes = selectContainer.querySelectorAll("input[name^='selection-']:checked");
        disable = selectedCheckboxes.length === 0;
      } else {
        if (pendingSelectedDates && pendingSelectedDates.length !== 2) {
          disable = true;
        }
      }
      
      if (okButton) {
        if (disable) {
          okButton.classList.add('disabled');
        } else {
          okButton.classList.remove('disabled');
        }
      }
    });
  }

  // 
  // If cancel, revert any selected operators since the drop-down was opened. Otherwise save.
  // 
  document.querySelectorAll(".button-container").forEach(buttonContainer => {
    const cancelButton = buttonContainer.querySelector(".cancel-button");
    const okButton = buttonContainer.querySelector(".ok-button");
  

    if (cancelButton) {
      cancelButton.addEventListener("click", function (event) {
        const selectContainer = event.target.closest(".select-container");
        if (selectContainer) {
          restoreInitialSelections(selectContainer);
        }
      });
    }
  
    if (okButton) {
      okButton.addEventListener("click", function (e) {
          selectionSaved = true;
      });
    }
  });


  // 
  // Clicking outside of ANY dropdown closes ALL dropdowns
  // 
  document.addEventListener("mousedown", function (e) {
    document.querySelectorAll(".select-container.active").forEach(selectContainer => {
      if (!selectContainer.contains(e.target)) {
        toggleDropdown(false, { target: selectContainer }); 
        restoreInitialSelections(selectContainer);
      }
    });
  });

  // hit esc to close active drop down
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      document.querySelectorAll(".select-container.active").forEach(selectContainer => {
        toggleDropdown(false, { target: selectContainer }); 
        restoreInitialSelections(selectContainer);
      });
  
    }
  });

  // 
  // Button click gradient behaviours
  // 

    function activateButtonPersistence(button) {
      const container = button.closest(".button-container");
      if (container) {
        const previous = activePersistentButtons.get(container);
        if (previous && previous !== button) {
          previous.classList.remove("persistent-active");
        }

        button.classList.add("persistent-active");
        activePersistentButtons.set(container, button);
      }      
    }
    window.activateButtonPersistence = activateButtonPersistence
    
    function deactivateButtonPersistence(button) {
      const container = button.closest(".button-container");
      if (container) {
        const previous = activePersistentButtons.get(container);
        if (previous) {
          previous.classList.remove("persistent-active");
        }
      }      
    }
    window.deactivateButtonPersistence = deactivateButtonPersistence





    const buttons = document.querySelectorAll(".ripple-btn");
    let isMouseDown = false;
    let activeButton = null;
    const activePersistentButtons = new Map();

    buttons.forEach(button => {
      // if click on button, remove previous ripple class (which retains dark blue background on hover)
      button.addEventListener("mousedown", function () {
          isMouseDown = true
          activeButton = button
          button.classList.add("ripple-active");

          if (button.classList.contains("persistent-btn")) {
            button.addEventListener("mouseup", function () {
              activateButtonPersistence(button)
            })
          }

      });

      // reset button if button un-clicked.
      document.addEventListener('mouseup', function() {
        // Non-persistent buttons have no timeout - with a timeout if you mouseup and quickly reenter the button you'll still see the effect disappear.
        // This isn't an issue for persistent buttons because the post-gradient state is maintained on click, and so wouldn't see the effect disappear.

        const timeout = activeButton && activeButton.classList.contains('persistent-btn') ? 200 : 0;
        // if (isMouseDown) {
          // delay so gradient has time to fade out
          activeButton?.classList.add('ripple-fade-out');

          setTimeout(() => {
            activeButton?.classList.remove('ripple-fade-out')
            activeButton?.classList.remove('ripple-active');
          }, 200);
          isMouseDown = false;
        // }
      });

      // Activating a button (still holding mouse button) and then moving away from the button will 'reset' the button colors
      // And while activated and re-hovering, the button will show the post-ripple dark blue colors. 
      button.addEventListener("mouseleave", function () {
        if (!isMouseDown) {
          button.classList.remove("ripple-active");
          return
        }
      });
    });
  

// 
// Debounce function to limit frequency of calls
// 
  function debounce(func, delay) {
    let timeout;
    return (...args) => {
      clearTimeout(timeout);
      timeout = setTimeout(() => func(...args), delay);
    };
  }


// 
// Apply gradient to long text strings in drop-downs
// 
  function longTextGradient() {
    const rows = document.querySelectorAll(".selection-row-wrapper");
    const dropdownSelections = document.getElementById("drop-down-body");
    const selectContainer = document.getElementById("select-container-operator");

    const initial_dropdownBorderWidth = parseFloat(window.getComputedStyle(dropdownSelections).getPropertyValue("border-right-width"))
    const checkboxWidth = document.querySelector(".drop-down-row.checkbox").getBoundingClientRect().width;

    if (!dropdownSelections || !selectContainer) return;

    // check overflow every 100ms while resizing select-container
    let dropdownWidth = 0;
    const resizeObserver = new ResizeObserver(debounce(entries => {
      dropdownWidth = entries[0].contentRect.width;
      checkOverflow();
    }, 0));

    resizeObserver.observe(selectContainer);


    function checkOverflow() {
      const dropdownScrolled = dropdownSelections.scrollLeft;

      rows.forEach(row => {
        const labelText = row.querySelector(".checkbox-label-text");

        const visibleTextWidth = dropdownWidth - checkboxWidth - initial_dropdownBorderWidth

        // start the gradient 70% into visible text
        const gradientStart = (visibleTextWidth + dropdownScrolled) * (0.7); 
        const gradientRemaining = dropdownWidth - gradientStart + dropdownScrolled;

        let gradientStops = '';
        const numStops = 10;
        for (let i = 0; i < numStops; i++) {
          const stopLen = gradientStart + (i * (gradientRemaining / numStops));
          const opacity = 1 - ((i + 1) / numStops); // Gradually decrease opacity from 1 to 0.1
          gradientStops += `rgba(0, 0, 0, ${opacity}) ${stopLen}px, `;
        }
        // Remove the trailing comma and space
        gradientStops = gradientStops.slice(0, -2);

        labelText.style.backgroundImage = `linear-gradient(to right, 
          black ${gradientStart}px, 
          ${gradientStops}
          )`;
        labelText.style.color = 'transparent';
        labelText.style.backgroundClip = "text";

      });
    }
  }

  // 
  // OVERFLOW FUNCTION TURNED OFF CURRENTLY:
  // 
  // Run on load, resize, and when dropdown is opened
  // window.addEventListener("load", longTextGradient);
  // window.addEventListener("resize", debounce(longTextGradient, 200)); 
  // document.getElementById("selectOperator").addEventListener("click", longTextGradient);
  
  // const dropdownSelections = document.getElementById("drop-down-body");
  // dropdownSelections.addEventListener("scroll", debounce(function() {longTextGradient();}, 0));


  // 
  // 'Select All' behaviour
  // 
    function syncSelectAll (selectAllCheckbox, checkboxes) {
      checkboxes.forEach(checkbox => {
        checkbox.checked = selectAllCheckbox.checked;
      });

      updateOkButtonState()
    }


    const clearAllbutton = document.querySelector(".clear-all-button")
    clearAllbutton.addEventListener("click", async function() {
      datepicker.clear();
  
      document.querySelectorAll(".select-container").forEach(selectContainer => {
        const selectAllCheckbox = selectContainer.querySelector(".checkbox_selectall input");
        const checkboxes = selectContainer.querySelectorAll("input[name^='selection-']");
        
        if (selectAllCheckbox) {
            selectAllCheckbox.checked = true;
            syncSelectAll(selectAllCheckbox, checkboxes);
        }
      });

      const forms = document.querySelectorAll(".form");
  
      // Manually trigger submit events so e.g. updateSelectedUI inside submit event listener runs
      forms.forEach(form => {
        const event = new Event("submit", { bubbles: true, cancelable: true });
        form.dispatchEvent(event);
      });
  
      // Combine all form data into one FormData - were overwriting each other otherwise and therefore not updating operator drop down correctly.
      const combinedFormData = new FormData();
      forms.forEach(form => {
        new FormData(form).forEach((value, key) => {
          combinedFormData.append(key, value);
        });
      });
  
      fetch("/", {
        method: "POST",
        body: combinedFormData
      });

      // update the various filter counts.
      updateSelectedUI()
      // update charts on filter select.
      await loadChartsForPage(currentPageKey)
    });

    document.querySelectorAll(".select-container").forEach(selectContainer => {
      const selectAllCheckbox = selectContainer.querySelector(".checkbox_selectall input");
      const checkboxes = selectContainer.querySelectorAll("input[name^='selection-']");

      if (!selectAllCheckbox || checkboxes.length === 0) return; // Skip if no checkboxes found

      selectAllCheckbox.addEventListener("change", function () {
        syncSelectAll(selectAllCheckbox, checkboxes);
      });

      // Uncheck 'Select All' if any given checkbox is unchecked.
      checkboxes.forEach(checkbox => {
        checkbox.addEventListener("change", function () {
          if (!this.checked) {
            selectAllCheckbox.checked = false;
          } else if ([...checkboxes].every(cb => cb.checked)) {
            selectAllCheckbox.checked = true;
          }
          updateOkButtonState()
        })
      });
  
    });


  const operatorCheckboxes = document.querySelectorAll("input[name='selection-operator']:checked");
  submittedOperators = Array.from(operatorCheckboxes).map(cb => cb.value); // initial selected Operators

  const phraseCheckboxes = document.querySelectorAll("input[name='selection-phrase']:checked");
  submittedPhrases = Array.from(phraseCheckboxes).map(cb => cb.value); // initial selected Operators


  function getHSLComponents(hslString) {
    // Regex to match HSL values (hue, saturation, lightness)
    // https://stackoverflow.com/questions/19289537/javascript-match-and-parse-hsl-color-string-with-regex
    const hslRegex = /hsl\((\d+),\s*(\d+)%\,\s*(\d+)%\)/;
    const match = hslString.match(hslRegex);
    
    if (match) {
        const hue = parseInt(match[1], 10);
        const saturation = parseInt(match[2], 10);
        const lightness = parseInt(match[3], 10);
        
        return {
            hue,
            saturation,
            lightness
        };
    }
    
    return null; 
  }


  window.assignSentimentAnHSL = function(sentiment) {
    const positiveColor = getCSSVar('positive-sentiment-text-color')
    const neutralColor = getCSSVar('neutral-sentiment-text-color')
    const negativeColor = getCSSVar('negative-sentiment-text-color')

    positiveHSL = getHSLComponents(positiveColor)
    neutralHSL = getHSLComponents(neutralColor)
    negativeHSL = getHSLComponents(negativeColor)

    let startColor, endColor;

    // Interpolate between positive and neutral if sentiment positive
    if (sentiment > 0) {
        startColor = neutralHSL;
        endColor = positiveHSL;
    } 
    // Interpolate between neutral and negative if sentiment negative
    else {
        startColor = neutralHSL;
        endColor = negativeHSL;
    }

    function interpolateHue(startHue, endHue, sAbs) {
      let hueDiff = endHue - startHue;
    
      if (Math.abs(hueDiff) > 180) {
        if (hueDiff > 0) {
          hueDiff -= 360;
        } else {
          hueDiff += 360;
        }
      }
    
      return (startHue + hueDiff * sAbs + 360) % 360;
    }

    // up the exponent if want to bias stronger - 1.5 or 2 is ok.
    const sAbs = Math.pow(Math.abs(sentiment), 1);

    const interpolatedH = interpolateHue(startColor.hue, endColor.hue, sAbs);
    const interpolatedS = startColor.saturation + (endColor.saturation - startColor.saturation) * sAbs;
    const interpolatedL = startColor.lightness + (endColor.lightness - startColor.lightness) * sAbs;

    return `hsl(${interpolatedH}, ${Math.round(interpolatedS)}%, ${Math.round(interpolatedL)}%)`;
  }


  let operatorDataCache = new Map();
  let shownOperator;
  let playTransition = true
  let defaultInfoStatRefreshMins = 5
  let hoveringOverInfoStatRefresh = false;

  const tooltip = document.getElementById('global-tooltip');
  const refreshStatuses = document.querySelectorAll('.stat-refresh-status');

  refreshStatuses.forEach(status => {
    const statusIcon = status.querySelector(".stat-rotation-time-since-icon")
    const statusIconPath = statusIcon.querySelector(".icon-path")

    status.addEventListener("mouseover", (e) => {
      hoveringOverInfoStatRefresh = true
      const rect = status.getBoundingClientRect();

      tooltip.style.opacity = 1;
  
      tooltip.style.top = `${rect.top + window.scrollY - tooltip.offsetHeight - 8}px`;
      tooltip.style.left = `${rect.left + window.scrollX + rect.width / 2}px`;

      statusIconPath.setAttribute("d", 'M17.65 6.35c-1.63-1.63-3.94-2.57-6.48-2.31-3.67.37-6.69 3.35-7.1 7.02C3.52 15.91 7.27 20 12 20c3.19 0 5.93-1.87 7.21-4.56.32-.67-.16-1.44-.9-1.44-.37 0-.72.2-.88.53-1.13 2.43-3.84 3.97-6.8 3.31-2.22-.49-4.01-2.3-4.48-4.52C5.31 9.44 8.26 6 12 6c1.66 0 3.14.69 4.22 1.78l-1.51 1.51c-.63.63-.19 1.71.7 1.71H19c.55 0 1-.45 1-1V6.41c0-.89-1.08-1.34-1.71-.71z')
    });
  
    status.addEventListener("mouseout", () => {
      hoveringOverInfoStatRefresh = false
      tooltip.style.opacity = 0;
      tooltip.style.pointerEvents = 'none';

      if (!statusIcon.classList.contains("loading")){
        statusIconPath.setAttribute("d", 'M11 8.75v3.68c0 .35.19.68.49.86l3.12 1.85c.36.21.82.09 1.03-.26.21-.36.1-.82-.26-1.03l-2.87-1.71v-3.4c-.01-.4-.35-.74-.76-.74s-.75.34-.75.75m10 .75V4.21c0-.45-.54-.67-.85-.35l-1.78 1.78c-1.81-1.81-4.39-2.85-7.21-2.6-4.19.38-7.64 3.75-8.1 7.94C2.46 16.4 6.69 21 12 21c4.59 0 8.38-3.44 8.93-7.88.07-.6-.4-1.12-1-1.12-.5 0-.92.37-.98.86-.43 3.49-3.44 6.19-7.05 6.14-3.71-.05-6.84-3.18-6.9-6.9C4.94 8.2 8.11 5 12 5c1.93 0 3.68.79 4.95 2.05l-2.09 2.09c-.32.32-.1.86.35.86h5.29c.28 0 .5-.22.5-.5')
      }
    });

    status.addEventListener("click", () => {
      updateInfoCards(activeOperators[currentIndex], defaultInfoStatRefreshMins, true)
    })
  });


  let statusesAndTooltipsUpdating = false
  function updateRefreshStatusesAndTooltips(paused = false) {
    let oldestTimestamp;

    // ensures that if paused AND manually refreshing, that the auto update on pause won't conflict with the manual refresh
    // if conflict -> minutes displays infinity.
    if (statusesAndTooltipsUpdating) return;
    statusesAndTooltipsUpdating = true

    if (paused) {
      const activeOp = activeOperators[currentIndex];
      const cacheData = operatorDataCache.get(activeOp?.OperatorName);
      if (!cacheData || typeof cacheData.timestamp !== 'number') {
        statusesAndTooltipsUpdating = false;
        return;
      }
      oldestTimestamp = cacheData.timestamp;
    } else {
      const allTimestamps = Array.from(operatorDataCache.values())
        .map(d => d.timestamp)
        .filter(ts => typeof ts === 'number');
  
      if (allTimestamps.length === 0) {
        statusesAndTooltipsUpdating = false;
        return;
      }
  
      oldestTimestamp = Math.min(...allTimestamps);
    }

    const currentTime = Date.now();
    const minutesRaw = (currentTime - oldestTimestamp) / (60 * 1000);
    const minutes = Math.floor(minutesRaw * 2) / 2;
    const minsLabel = minutes === 0 ? "<0.5m" : `${minutes}m`;
    
    refreshStatuses.forEach(status => {
      const statusText = status.querySelector(".stat-rotation-time-since-text");
      if (statusText) {
        statusText.textContent = minsLabel;
      }
    });

    tooltip.textContent = `Oldest stats ${minsLabel} old. Refreshes every ${defaultInfoStatRefreshMins}m.`;

    statusesAndTooltipsUpdating = false
  }





  async function updateInfoCards(operator, getDataIfElapsed = defaultInfoStatRefreshMins, clearCache = false) {
    // getDataIfElapsed is in mins.
    const fadeTransitionDelay = 300
  
    // Overall Sentiment Section
    const overallSentimentProcessedPct = document.getElementById("overall-sentiment-processed-pct");
    const overallSentimentAverageSentiment = document.getElementById("overall-sentiment-average-sentiment");
    const overallSentimentHighestSentiment = document.getElementById("overall-sentiment-highest-sentiment");
    const overallSentimentLowestSentiment = document.getElementById("overall-sentiment-lowest-sentiment");
  
    const mostPositivePhraseName = document.getElementById("most-positive-phrase-name");
    const mostPositivePhraseSentiment = document.getElementById("most-positive-phrase-sentiment");

    const mostNegativePhraseName = document.getElementById("most-negative-phrase-name");
    const mostNegativePhraseSentiment = document.getElementById("most-negative-phrase-sentiment");
    
    // Phrase Sentiment Section 
    // ... 
  
    // Summary Section
    // ...
  
    const textElements = [overallSentimentProcessedPct, overallSentimentAverageSentiment, overallSentimentHighestSentiment, overallSentimentLowestSentiment,
                          mostPositivePhraseSentiment, mostPositivePhraseName, mostNegativePhraseSentiment, mostNegativePhraseName];
    
    playTransition = true
    // if staying on same operator, don't transition.
    if (shownOperator == operator && !clearCache) {
      playTransition = false
    }
    shownOperator = operator
    
    if (playTransition) {
      textElements.forEach(el => {
        el.classList.remove("fade-in");
        el.classList.add("fade-out");
      });
    }

    const currentTime = Date.now();


    if (clearCache) {
      operatorDataCache.clear();
    }
    const cacheData = operatorDataCache.get(operator.OperatorName);


    if (cacheData && (currentTime - cacheData.timestamp < getDataIfElapsed * 60 * 1000)) {
      
      // Use cached data if available and not older than getDataIfElapsed mins
      setTimeout(() => {
        updateContentWithOperatorStats(cacheData.stats);
      }, fadeTransitionDelay);

      // console.log('using cached data', operator.OperatorName, new Date().toLocaleTimeString())

      updateRefreshStatusesAndTooltips()

    } else {
      // if no cached data within the specified timeframe for the operator, refresh it. 
      tooltip.textContent = 'Refreshing...'
      refreshStatuses.forEach(status => {
        const statusText = status.querySelector(".stat-rotation-time-since-text")
        const statusIcon = status.querySelector(".stat-rotation-time-since-icon")
        const statusIconPath = statusIcon.querySelector(".icon-path")

        statusText.textContent = '...'


        statusIcon.classList.add('loading')
        statusIconPath.setAttribute("d", 'M17.65 6.35c-1.63-1.63-3.94-2.57-6.48-2.31-3.67.37-6.69 3.35-7.1 7.02C3.52 15.91 7.27 20 12 20c3.19 0 5.93-1.87 7.21-4.56.32-.67-.16-1.44-.9-1.44-.37 0-.72.2-.88.53-1.13 2.43-3.84 3.97-6.8 3.31-2.22-.49-4.01-2.3-4.48-4.52C5.31 9.44 8.26 6 12 6c1.66 0 3.14.69 4.22 1.78l-1.51 1.51c-.63.63-.19 1.71.7 1.71H19c.55 0 1-.45 1-1V6.41c0-.89-1.08-1.34-1.71-.71z')


      });
      const start = Date.now();
      
      const res = await fetch(`/api/infostats/overall_sentiment?current_operatorID_in_rotation=${encodeURIComponent(operator.OperatorID)}`);
      const operatorStatsJSON = await res.json();
      const operatorStats = operatorStatsJSON.find(op => op.OperatorName === operator.OperatorName);
      
      const elapsed = Date.now() - start;
      const delay = Math.max(0, fadeTransitionDelay - elapsed); // ensure fadeTransitionDelay's ms transition even if api response faster than that
  
      // console.log('new data', operator.OperatorName, new Date().toLocaleTimeString())
      // Cache new
      operatorDataCache.set(operator.OperatorName, {
        stats: operatorStats,
        timestamp: currentTime,
      });
  
      setTimeout(() => {
        updateContentWithOperatorStats(operatorStats);
      }, delay);

      refreshStatuses.forEach(status => {
        const statusText = status.querySelector(".stat-rotation-time-since-text")
        const statusIcon = status.querySelector(".stat-rotation-time-since-icon")
        const statusIconPath = statusIcon.querySelector(".icon-path")

        let stopAfterNextSpin = false;

        const handleSpinDone = () => {
          if (stopAfterNextSpin) {
            statusIcon.classList.remove("loading");
            statusIcon.removeEventListener("animationiteration", handleSpinDone);

          }
        };
        statusIcon.removeEventListener("animationiteration", handleSpinDone);
        statusIcon.addEventListener("animationiteration", handleSpinDone);

        if (!hoveringOverInfoStatRefresh) {
          statusIconPath.setAttribute("d", 'M11 8.75v3.68c0 .35.19.68.49.86l3.12 1.85c.36.21.82.09 1.03-.26.21-.36.1-.82-.26-1.03l-2.87-1.71v-3.4c-.01-.4-.35-.74-.76-.74s-.75.34-.75.75m10 .75V4.21c0-.45-.54-.67-.85-.35l-1.78 1.78c-1.81-1.81-4.39-2.85-7.21-2.6-4.19.38-7.64 3.75-8.1 7.94C2.46 16.4 6.69 21 12 21c4.59 0 8.38-3.44 8.93-7.88.07-.6-.4-1.12-1-1.12-.5 0-.92.37-.98.86-.43 3.49-3.44 6.19-7.05 6.14-3.71-.05-6.84-3.18-6.9-6.9C4.94 8.2 8.11 5 12 5c1.93 0 3.68.79 4.95 2.05l-2.09 2.09c-.32.32-.1.86.35.86h5.29c.28 0 .5-.22.5-.5')
        }

        stopAfterNextSpin = true;

        tooltip.textContent = `Oldest stats <0.5m old. Refreshes every ${defaultInfoStatRefreshMins}m.`
        statusText.textContent = '<0.5m'

      });
      
    }
  

    function updateContentWithOperatorStats(operatorStats) {
      overallSentimentProcessedPct.innerText = `${operatorStats.ProcessedReviewRatio}%`;


      function applyBoundsAndSentimentColor(element, value, lowerBound = -1, upperBound = 1) {
        const lowerBoundHTML = `<span class="sentiment-lower-bound-text info-card-bound">${lowerBound}</span>`;
        const upperBoundHTML = `<span class="sentiment-upper-bound-text info-card-bound">${upperBound}</span>`;
        const zeroBoundHTML = `<span class="sentiment-zero-bound-text info-card-bound">${0}</span>`;
      
        // if bound, gets underline styling for clarity.
        const valueHTML = `<span class="sentiment-info-stat-is-bound">${value}</span>`;

        let result = '';
      
        if (value === upperBound) {
          result = `${lowerBoundHTML} ${zeroBoundHTML} ${valueHTML}`;
        } else if (value === lowerBound) {
          result = `${valueHTML} ${zeroBoundHTML} ${upperBoundHTML}`;
        } else {
          result = `${lowerBoundHTML} ${value} ${upperBoundHTML}`;
        }

        element.dataset.sentimentValue = value;
        element.style.color = assignSentimentAnHSL(value);

        return result;
      }

      overallSentimentAverageSentiment.innerHTML = applyBoundsAndSentimentColor(overallSentimentAverageSentiment, operatorStats.AvgSentimentScore);

      overallSentimentHighestSentiment.innerText = `${operatorStats.PositiveReviewPercentage}%`;
      overallSentimentLowestSentiment.innerText = `${operatorStats.CountReviews}`;
  
      mostPositivePhraseName.innerText = `${operatorStats.MostPositivePhraseName}`
      mostPositivePhraseSentiment.innerHTML = applyBoundsAndSentimentColor(mostPositivePhraseSentiment, operatorStats.MostPositivePhraseSentiment);

      mostNegativePhraseName.innerText = `${operatorStats.MostNegativePhraseName}`
      mostNegativePhraseSentiment.innerHTML = applyBoundsAndSentimentColor(mostNegativePhraseSentiment, operatorStats.MostNegativePhraseSentiment);
 
      if (playTransition) {
        textElements.forEach(el => {
          el.classList.remove("fade-out");
          el.classList.add("fade-in");
        });
      }

    }
  }

  let rotationStartTime;
  let currentIndex = 0;
  let animationFrameId;
  // Rotate through operator stats. 
  // operators is defined in index.html (... from session) 
  let activeOperators = operators.filter(op => submittedOperators.includes(op.OperatorName));

  function cycleOperators() {
    currentIndex = (currentIndex + 1) % activeOperators.length;
    updateInfoCards(activeOperators[currentIndex]);

    // if processing, don't delay data retrieval (get data for operator when rotated to)
    // updateInfoCards(activeOperators[currentIndex], getDataIfElapsed = 0);

    rotationStartTime = performance.now();
    cancelAnimationFrame(animationFrameId);

    animateProgressBar();
    updateSelectedUI(currentIndex)

  }

  let interval;
  const statRotationDuration = 7500 // ms

  function startInfoStatCycle() {
    rotationStartTime = performance.now();
    animateProgressBar();

    interval = setInterval(cycleOperators, statRotationDuration);
  }

  function stopInfoStatCycle() {
    clearInterval(interval);

    // reset progress to 0
    const progressBars = document.querySelectorAll('.stat-rotation-progress-bar');
    progressBars.forEach(bar => {
      bar.style.width = '0';
    });
  }


  function animateProgressBar() {
    const progressBars = document.querySelectorAll('.stat-rotation-progress-bar');

    if (!progressBars.length) return;

    if (activeOperators.length == 1) {
      progressBars.forEach(bar => {
      bar.style.width = '100%';
      });
      return;
    }

    const now = performance.now()
    const elapsed = now - rotationStartTime;
    let percent = Math.min((elapsed / statRotationDuration) * 100, 100);

    progressBars.forEach(bar => {
      bar.style.width = `${percent}%`;
    });

    if (percent < 100) {
      animationFrameId = requestAnimationFrame(animateProgressBar);
    }

  }
  
  let isPaused = false
  function updateSelectedUI() {
    const buttonsContainers = document.querySelectorAll(".stat-progress-container")

    function updateCountAndPlural(count, statSelector, pluralSelector) {
      document.querySelectorAll(statSelector).forEach(el => {
        el.innerText = count;
      });
    
      const plural = count === 1 ? '' : 's';
    
      document.querySelectorAll(pluralSelector).forEach(el => {
        const base = el.dataset.baseText || el.innerText.replace(/s$/, '');
        el.innerText = base + plural;
        el.dataset.baseText = base;
      });
    }

    updateCountAndPlural(activeOperators.length, '.active-operators-stat', '.active-operator-plural')
    updateCountAndPlural(submittedPhrases.length, '.active-phrases-stat', '.active-phrase-plural');


    const formatDate = (date) => date.toLocaleString('sv').split(" ")[0];

    // defaults to the... default... min/max date (being the min/max date for the selected operators)
    let minSelectedDate = formatDate(datepicker.config.minDate)
    let maxSelectedDate = formatDate(datepicker.config.maxDate)

    if (submittedDates.length) {
      // if dates selected, those are the dates shown in the UI.
      minSelectedDate = formatDate(submittedDates[0]);
      maxSelectedDate = formatDate(submittedDates[1]);
    }
    
    const activeMinDateStats = document.querySelectorAll('.active-min-date-stat')
    const activeMaxDateStats = document.querySelectorAll('.active-max-date-stat')

    activeMinDateStats.forEach(el => el.innerText = minSelectedDate);
    activeMaxDateStats.forEach(el => el.innerText = maxSelectedDate);


    buttonsContainers.forEach(buttonsContainer => {
      const btn = buttonsContainer.querySelector(".stat-rotation-progress-text")
      const indexTextEl = btn.querySelector(".rotation-index");
      const catTextEl = btn.querySelector(".rotation-category-name");

      const icon = btn.querySelector(".pause-icon");

      const indicator = buttonsContainer.querySelector(".stat-rotation-progress-bar")

      if (activeOperators.length === 1) {
        icon.style.visibility = 'hidden'
        indexTextEl.classList.remove('show')

        indexTextEl.innerText = '1'
        
        catTextEl.innerText = `${activeOperators[currentIndex].OperatorName}`

        indicator.style.width  = '100%'
      }
      else {
        icon.style.visibility = 'visible'
        indexTextEl.classList.add('show')

        indexTextEl.innerText = `${currentIndex + 1} of ${activeOperators.length}`
        catTextEl.innerText = `${activeOperators[currentIndex].OperatorName}`


        if (isPaused) {
          icon.classList.add('show');
        } else {
          icon.classList.remove('show');
        }
      }
    })

    function getPlaceholder(count, total, label) {
      const plural = count > 1 ? 's' : ''
      return count === total
        ? `All ${label}s Selected`
        : `${count} ${label}${plural} Selected`;
    }
    
    document.getElementById('operatorInput').placeholder = getPlaceholder(
      activeOperators.length,
      operators.length,
      'Operator'
    );
    
    document.getElementById('phraseInput').placeholder = getPlaceholder(
      submittedPhrases.length,
      phrases.length,
      'Phrase'
    );

  }

  // Update the UI with the initial selected operators
  updateSelectedUI(currentIndex);

  // PREVENTS FORM FROM REFRESHING PAGE (and instead update UI / send data to backend without refresh)
  const forms = document.querySelectorAll(".form");
  
  forms.forEach(form => {
    form.addEventListener("submit", async function (event) {
      event.preventDefault(); // Stop default form behavior
      const formData = new FormData(form);

      await fetch("/", {
        method: "POST",
        body: formData
      });

      // update and restart info stat rotation on submission. 
      if (form.id === 'operator-form') {
        submittedOperators = [...formData.getAll('selection-operator')];
        activeOperators = operators.filter(op => submittedOperators.includes(op.OperatorName));
        
        currentIndex = 0 
        updateInfoCards(activeOperators[currentIndex], defaultInfoStatRefreshMins, true);
    
        stopInfoStatCycle()
        cancelAnimationFrame(animationFrameId);
        startInfoStatCycle()

      }
      else if (form.id === 'phrase-form') {
        submittedPhrases = [...formData.getAll('selection-phrase')];

      } 
      else if (form.id === 'daterange-form') {
        submittedDates = pendingSelectedDates
        submittedDatePreselectButton = pendingDatePreselectButton
      } 
      
      updateSelectedUI();

      collapseExpandedCardClone()
      await loadChartsForPage(currentPageKey);

    });
  })

  // Initial load
  updateInfoCards(activeOperators[currentIndex]);
  startInfoStatCycle();

  // on info section hover, stop progress (as to allow user to 'focus' on content.) TODO: consider pausing instead.
  let hoverPause = false;
  let manualPause = false;
  let hoverIntervalId = null

  const infoSections = document.querySelectorAll('.stat-info-wrapper');
  const statIcons = document.querySelectorAll('.stat-start-stop-icon');
  
  function updatePauseState() {
    const newPauseState = hoverPause || manualPause;
  
    if (newPauseState !== isPaused) {
      isPaused = newPauseState;
  
      if (isPaused) {

        hoverIntervalId = setInterval(() => {
          updateRefreshStatusesAndTooltips(true);
        }, 1000);

        stopInfoStatCycle();
        cancelAnimationFrame(animationFrameId);
      } else {

        if (hoverIntervalId !== null) {
          clearInterval(hoverIntervalId);
          hoverIntervalId = null;
        }

        startInfoStatCycle();
      }
  
      updateSelectedUI(); // update button state
    }
  }


  infoSections.forEach(infoSection => {
    infoSection.addEventListener('mouseenter', function () {
      hoverPause = true;
  
      if (!manualPause) {
        // Lower opacity for all 'pause' icons when hovering over the section and not manually paused
        statIcons.forEach(icon => {
          icon.style.opacity = '0.3';
        });
      }
  
      updatePauseState();
    });
  
    infoSection.addEventListener('mouseleave', function () {
      hoverPause = false;
  
      if (!manualPause) {
        // Reset opacity for all icons when leaving the section and not manually paused
        statIcons.forEach(icon => {
          icon.style.opacity = '';
        });
      }
      updatePauseState();
    });
  });
  
  statIcons.forEach(icon => {
    icon.addEventListener('mouseover', function () {
      if (!manualPause && hoverPause) {
        // Increase opacity of all pause icons when hovering over any one pause icon, and not manually paused
        statIcons.forEach(icon => {
          icon.style.opacity = '0.7';
        });
      }
    });
  
    icon.addEventListener('mouseleave', function () {
      if (!manualPause && hoverPause) {
        // Revert opacity of all icons when mouse leaves any icon and not manually paused
        statIcons.forEach(icon => {
          icon.style.opacity = '0.3';
        });
      }
    });
  
    icon.addEventListener('click', function () {
      manualPause = !manualPause;
  
      // reset opacity on click (will only apply when play icon is shown as pause behaviours handled above)
      statIcons.forEach(icon => {
        icon.style.opacity = '';
      });
      
      updatePauseState();
    });
  });

  // 
  // Nav bar item indicator
  // 

  const navItems = document.querySelectorAll(".nav-item");

  navItems.forEach((item, index) => {
    item.addEventListener("click", () => updatePageIndicator(index));
  });

  window.addEventListener("resize", () => {
    updatePageIndicator(activePageIndex, false);
  });

  window.updatePageIndicator = updatePageIndicator;
  updatePageIndicator(0); 


// 
// toggle start/stop icon on click
// 

function switchSvgIconOnStartStop(svgClass, dStart, dStop, rotationDegrees = 90, initialRotation = 0, changeAll = false) {
  const svgs = document.querySelectorAll(svgClass);

  svgs.forEach(svg => {
    const path = svg.querySelector(".icon-path");

    // Set initial rotation
    path.style.transform = `rotate(${initialRotation}deg)`;
    path.style.transition = 'transform 0.3s ease';

    svg.addEventListener('click', function () {
      const isPlay = path.getAttribute("d") === dStart;
      const processContainer = svg.closest('.begin-processing-container')


      if (svgClass === '.processing-start-stop-icon') {
        currentlyProcessing = false
        document.querySelectorAll('.begin-processing-container').forEach(container => {
          container.classList.remove('active');
        });
      }


      updateProcessingContainerHeight()

      if (isPlay) {
        
        if (svgClass === '.processing-start-stop-icon') {
          currentlyProcessing = true

          processContainer.classList.add('active')
          // disable other svgs.
          svgs.forEach(otherSvg => {
            if (otherSvg !== svg) {
              otherSvg.classList.add('disabled');
            } else {
              otherSvg.classList.remove('disabled');
            }
          });
        }

        // change to 'stop' icon
        path.setAttribute("d", dStop);
        path.style.transform = `rotate(${rotationDegrees}deg)`;
        
        // If changeAll is true, update other icons with same new svg
        if (changeAll) {
          currentlyProcessing = false

          svgs.forEach(otherSvg => {
            const otherPath = otherSvg.querySelector(".icon-path");
            otherPath.setAttribute("d", dStop);
            otherPath.style.transform = `rotate(${rotationDegrees}deg)`;
          });
        }

      } else {
        if (svgClass === '.processing-start-stop-icon') {
          processContainer.classList.remove('active')

          svgs.forEach(otherSvg => {
            otherSvg.classList.remove('disabled');
          });
        }
        
        // back to 'start' icon
        path.setAttribute("d", dStart);
        path.style.transform = `rotate(${initialRotation}deg)`;
        
        if (changeAll) {
          svgs.forEach(otherSvg => {
            const otherPath = otherSvg.querySelector(".icon-path");
            otherPath.setAttribute("d", dStart);
            otherPath.style.transform = `rotate(${initialRotation}deg)`;
          });
        }
      }
    });
  });
}

  switchSvgIconOnStartStop(
    // begin-processing-container
    svgClass = '.processing-start-stop-icon', 
    dStart = "M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2M9.5 16.5v-9l7 4.5z",
    dStop = "M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2m4 14H8V8h8z"
  )

  switchSvgIconOnStartStop(
    // processing tab info card rotators: hover 
    svgClass = '.stat-start-stop-icon', 
    dStart = "M6 19h4V5H6zm8-14v14h4V5z",
    dStop = "M8 5v14l11-7z",
    rotationDegrees = 0,
    initialRotation = -180,
    changeAll = true
  )


  function updateProcessingContainerHeight() {
    const resizeHandler = () => {
      const sections = document.querySelectorAll('.process-section');
  
      sections.forEach(section => {
        const processContainer = section.querySelector('.begin-processing-container');
        const infoSection = section.querySelector('.info-section.inner');
        const firstInfoCard = infoSection?.querySelector('.first-info-card');
        
        processContainer.addEventListener('mouseenter', function() {

          if (firstInfoCard && processContainer && infoSection) {
            const infoSectionTopMargin = parseFloat(getComputedStyle(infoSection).marginTop) || 0;
            const infoCardHeight = firstInfoCard.offsetHeight;
    
            // on hover (and activation), set height of the processing container to cover the first info card (because the card will be absorbed
            // into the section)
            processContainer.style.height = `${infoCardHeight + infoSectionTopMargin}px`;
          }
        });


        processContainer.addEventListener('mouseleave', function() {
          if (!processContainer.classList.contains('active')) {
            processContainer.style.height = '7%'
          }
        })

      });
    };
  
    resizeHandler();
    window.addEventListener('resize', resizeHandler);
  }
  
  updateProcessingContainerHeight();




  // https://ascii.today/ - Doom by Frans P. de Vries
  // _____ _                _   _             
  // /  __ \ |              | | (_)            
  // | /  \/ |__   __ _ _ __| |_ _ _ __   __ _ 
  // | |   | '_ \ / _` | '__| __| | '_ \ / _` |
  // | \__/\ | | | (_| | |  | |_| | | | | (_| |
  //  \____/_| |_|\__,_|_|   \__|_|_| |_|\__, |
  //                                      __/ |
  //                                     |___/
  // eventually split out into own js.

  const infoSection = document.querySelector('.dashboard-graph .info-section');
  const infoCards = [...infoSection.querySelectorAll('.info-card')];
  // set Grid Layout for chart containers.
  function updateGridLayout(visibleCards = null) {
    if (!visibleCards) {
      visibleCards = infoCards.filter(card => !card.classList.contains('shrunk'));
    }
    const total = visibleCards.length;
    if (total === 0) return;
    
    switch (total) {
      case 1:
        // 1 card: fill section.
        infoSection.style.gridTemplateColumns = '1fr';
        infoSection.style.gridTemplateRows = '1fr';
        infoSection.style.gridTemplateAreas = `'1'`;
        visibleCards[0].style.gridArea = '1';
        break;
      case 2:
        // 2 card: 1 col 2 rows, cards fill their row.
        infoSection.style.gridTemplateColumns = '1fr';
        infoSection.style.gridTemplateRows = '1fr 1fr';
        infoSection.style.gridTemplateAreas = `'a' 'b'`;
        visibleCards[0].style.gridArea = 'a';
        visibleCards[1].style.gridArea = 'b';
        break;
      case 3:
        // 3 cards: first row has 2 cards, second row has 1 full width card. 
        infoSection.style.gridTemplateColumns = '1fr 1fr';
        infoSection.style.gridTemplateRows = '1fr 1fr';
        infoSection.style.gridTemplateAreas = `'a b' 'c c'`;
        visibleCards[0].style.gridArea = 'a';
        visibleCards[1].style.gridArea = 'b';
        visibleCards[2].style.gridArea = 'c';
        break;
      case 4:
        // 4 cards: 2x2 grid.
      default:
        infoSection.style.gridTemplateColumns = '1fr 1fr';
        infoSection.style.gridTemplateRows = '1fr 1fr';
        infoSection.style.gridTemplateAreas = `'a b' 'c d'`;
        visibleCards[0].style.gridArea = 'a';
        visibleCards[1].style.gridArea = 'b';
        visibleCards[2].style.gridArea = 'c';
        visibleCards[3].style.gridArea = 'd';
        break;
    }

  }

  const dashboardNavButtons = document.querySelectorAll('.dashboard-nav button')
  dashboardNavButtons.forEach(button => {
    button.addEventListener('click', function () {
      const pageId = button.id.replace(/-btn$/, '');
      loadChartsForPage(pageId, forceChartReload = true)
    });
  });


  let activeClone = null;
  let originalCard = null;


  
  let cardsShrunkWithExpansion = [];
  // 
  // when a card is clicked, (clone it and) expand it to fill the full section.
  //
  function expandCard(card) {
    const { offsetTop: top, offsetLeft: left, offsetWidth: width, offsetHeight: height } = card

    const clone = card.cloneNode(true);
    clone.classList.add('clone-card');    

    // no need to exapnd if already filling the grid!
    if (currentPageVisibleCharts.length === 1) return;

    // set the dimensions and position of the cloned card equal to that of the original card.
    // the original card is cloned to facilitate smooth transitions (which aren't possible with other grid/display methods.)
    // ...
    // this has some very weird behaviour after updateGridLayout() running which I don't really understand - even though position absolute is set, 
    // offsetTop/left gives a value outside of the card. Therefore use top 0, left 0 which is relative to the card - not the section as it should be.
    Object.assign(clone.style, {
      position: 'absolute',
      top: '0',
      left: '0',
      width: `${width}px`,
      height: `${height}px`,
      transition: 'height 0.3s ease, width 0.3s ease, top 0.3s ease, left 0.3s ease',
    });
    
    originalCard = card;
    // Shrink other cards,
    // only shrink cards if they aren't shrunk by the layout setting (i.e. if layout contains 3 cards, 1 is already shrunk, don't want to affect that)
    cardsShrunkWithExpansion = [];
    infoCards.forEach(c => {
      if (c !== originalCard && !c.classList.contains('shrunk')) {
        c.classList.add('shrunk', 'hidden');
        cardsShrunkWithExpansion.push(c);
      }
    });

    // clone chart as well.
    const originalCanvas = card.querySelector('canvas');
    const clonedCanvas = clone.querySelector('canvas');

    if (originalCanvas && clonedCanvas && window.Chart) {
      const chartConfig = Chart.getChart(originalCanvas)?.config;

      if (chartConfig) {
        // Temporarily disable animations for the cloned chart - which is only animated on resize.
        // so this turns off resize animation for the cloned chart which looked awkward (as the bars animated from the center outwards)
        const originalAnimation = chartConfig.options.animation;
        chartConfig.options.animation = false;
      
        new Chart(clonedCanvas.getContext('2d'), chartConfig);
        
        // reset animation for the original chart
        chartConfig.options.animation = originalAnimation;
      }
    }
    
    infoSection.appendChild(clone);
    activeClone = clone;
  
    // 
    // expand clone to fill entire section.
    // ...
    // as before RE: weird layout after updateGridLayout(), offset the weird values by flipping them and get the proper values for what should be the 
    // relative element (infoSection)
    requestAnimationFrame(() => {
      Object.assign(clone.style, {
        top: `-${top}px`,
        left: `-${left}px`,
        width: `${infoSection.offsetWidth}px`,
        height: `${infoSection.offsetHeight}px`,
        maxHeight: `${infoSection.offsetHeight}px`
      });
    });

     expandIcon = clone.querySelector('.graph-expand-icon')
     expandIcon.innerHTML = `<path fill="${getCSSVar('accent-color')}" d="M7.41 18.59 8.83 20 12 16.83 15.17 20l1.41-1.41L12 14zm9.18-13.18L15.17 4 12 7.17 8.83 4 7.41 5.41 12 10z"/>`;

     expandIcon.addEventListener('click', collapseExpandedCardClone);
  }
  
  function collapseExpandedCardClone() {
    if (!activeClone || !originalCard) return;
  
    // set back to original position
    const { offsetWidth: width, offsetHeight: height } = originalCard;
    Object.assign(activeClone.style, {
      top: '0',
      left: '0',
      width: `${width}px`,
      height: `${height}px`,
      
    });
  
    // Wait for animation to complete, then remove clone.
    setTimeout(() => {
      if (activeClone) {
        activeClone.remove();
        activeClone = null;
      }
      if (originalCard) {
        originalCard = null;
      }
    }, 400);

    cardsShrunkWithExpansion.forEach(c => c.classList.remove('shrunk', 'hidden'));
    cardsShrunkWithExpansion = [];
  }


  const drawZeroLinePlugin = {
    id: 'drawZeroLine',
    beforeDraw(chart, args, options) {
      const { ctx, scales } = chart;

      if (!scales.y || !scales.x) return

      const yScale = scales.y;
      const xScale = scales.x;
  
      const yZero = yScale.getPixelForValue(0);
  
      ctx.save();
      ctx.beginPath();
      ctx.moveTo(xScale.left, yZero);
      ctx.lineTo(xScale.right, yZero);
      ctx.lineWidth = 1;
      ctx.strokeStyle = options.color;
      ctx.stroke();
      ctx.restore();
    }
  };
  Chart.register(drawZeroLinePlugin);
  



  function renderChartById(id, chartConfig = {}) {
    const canvas = document.getElementById(id);

    if (!canvas) {
      console.warn(`chart with ID "${id}" not found.`);
      return;
    }
  
    // looks up chart class from config 
    const ctx = canvas.getContext('2d');
    const chartType = chartConfig.type || 'bar';
  
    const defaultOptions = {
      // for resize behaviour
      responsiveAnimationDuration: 5000,

      maintainAspectRatio: false,
      plugins: {
        legend: { display: true },
        drawZeroLine: {}
      },
      scales: {
        x: {
          type: chartType === 'scatter' ? 'linear' : 'category',
          ticks: {
            // if x axis label greater than specified chars, truncate.
            callback: function (value) {
              const label = this.getLabelForValue(value);
              const limit = 15;
              return typeof label === 'string' && label.length > limit ? label.slice(0, limit - 3) + '…' : label;
            }
          },
          title: {
            display: true,
            text: chartConfig.xTitle || 'X Axis',
            font: { size: 14 }
          }
        },
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: chartConfig.yTitle || 'Y Axis',
            font: { size: 14 }
          }
        }
      }
    };
      
    let mergedOptions = { ...defaultOptions, ...chartConfig.options };

  Object.keys(mergedOptions.scales || {}).forEach(scaleKey => {
    if (scaleKey.startsWith('y')) {
      const scaleConfig = mergedOptions.scales[scaleKey];

      if (!scaleConfig.suggestedMin && chartConfig.data?.datasets?.length) {
        const values = chartConfig.data.datasets.flatMap(ds => {
          return Array.isArray(ds.data)
            ? ds.data.map(point => typeof point === 'object' ? point.y : point)
            : [];
        });

        const maxAbs = Math.max(...values.map(Math.abs));
        const hasNegative = values.some(v => v < 0);

        // if data has negative values, y values are symmetric around y = 0. Otherwise, start at y = 0.
        scaleConfig.suggestedMin = hasNegative ? -maxAbs : 0;
        scaleConfig.suggestedMax = maxAbs;
      }
    }
  });

  const dataLabelPlugin = (chartType === 'pie') ? [ChartDataLabels] : [];
  // register outer labels for pie charts only.

  // Create the chart with merged options
  const chart = new Chart(ctx, {
    type: chartType,
    data: chartConfig.data,
    options: mergedOptions,
    plugins: dataLabelPlugin
  });

    window[id] = chart
    updateChartTheme(chart)
  }

  const pageChartConfigs = {
    overviewPage: {
      chart1: {
        options: {
          responsive: true,
          plugins: {
            title: {
              display: true,
              text: 'Operator: Review vs Sentiment Score'
            },
            legend: {
              position: 'top'
            }
          },
          scales: {
            x: {
              title: {
                display: true,
                text: 'Operator'
              }
            },
            y: {
              title: {
                display: true,
                text: 'Average Review Rating'
              },
              suggestedMin: 0,
              suggestedMax: 5
            },
            ySentiment: {
              type: 'linear',
              position: 'right',
              title: {
                display: true,
                text: 'Avg Sentiment Score'
              },
              grid: {
                drawOnChartArea: false // to avoid overlapping grid lines
              },
              suggestedMin: -1,
              suggestedMax: 1,
            }
          }
        },
        type: 'bar',
        title: 'Average Sentiment by Operator',
        xTitle: 'Operator',
        // yTitle: 'Average Review Rating',
        api: '/api/graph/reviewrating_by_overall_sentiment_data',
        mapData: data => ({
          labels: data.map(d => d.OperatorName),
          datasets: [
            {
              label: 'Avg Review Rating',
              data: data.map(d => d.AvgReviewRating),
              pointRadius: 6,
              order: 2
            },
            {
              label: 'Avg Sentiment Score',
              data: data.map(d => d.AvgSentimentScore),
              yAxisID: 'ySentiment',
              backgroundColor: getCSSVar('positive-sentiment-text-color'),
              // borderColor: 'blue', // line color if true
              type: 'line',
              showLine: false,
              pointRadius: 6,
              order: 1
            }
          ]
        })
      },

      chart2: {
        type: 'pie',
        api: '/api/graph/overall_sentiment_portion',
        options: {
          plugins: {
            tooltip: {
              callbacks: {
                label: (context) => {
                  const value = context.raw;
                  // Get total value of the dataset
                  const total = context.chart._metasets[0].total;
                  const percentage = ((value / total) * 100).toFixed(1);

                  // thousands separator
                  const formattedValue = value.toLocaleString();

                  return `${formattedValue} (${percentage}%)`;
                }
              }
            },
            title: {
              display: true,
              text: 'Portion of Sentiment Scores over Selected Operators',
              padding: {
                bottom: 40
              },
            },
            legend: {
              display: false
            },
            datalabels: {
              // label color set in updateChartTheme()
              font: {
                size: 14,
                weight: 'bold',
              },
              anchor: 'end',
              align: 'end',
              offset: 10, 
              clamp: true,
              formatter: (value, context) => {
                return context.chart.data.labels[context.dataIndex];
              }
            }
          },
          layout: {
            padding: {
              top: 10,
              bottom: 30
            }
          },
          scales: {
            y: {
              display: false
            }
          },
          elements: {
            arc: {
              // borderColor: getCSSVar('tertiary-color'),
              borderWidth: 0 // segment gap
            }
          }
        },
        mapData: data => ({
          labels: data.map(d => d.SentimentScore),
          datasets: [{
            label: 'Count Sentiment Score',
            data: data.map(d => d.CountSentimentScore)
            // segment color set in updateChartTheme()
          }]
        })

      },

      chart3: {
        type: 'line',
        api: '/api/graph/AvgSentimentOverTime',
        mapData: data => {
          const ratings = [3, 4, 5];
          const colors = ['#FF5733', '#FFBD33', '#33FF57', '#3377FF', '#9933FF'];
          
          return {
            labels: data.map(d => d.ReviewYear),
            datasets: ratings.map((rating, i) => ({
              label: `Rating: ${rating}`,
              borderColor: colors[i],
              backgroundColor: colors[i],
              fill: false,
              tension: 0.5, // the 'curvature' of the line
              pointHoverRadius: 5,
              pointHitRadius: 10,
              pointRadius: 1,
              data: data.map(d => {
                const entry = data.find(e => 
                  Number(e.ReviewYear) === Number(d.ReviewYear) && 
                  Number(e.ReviewRating) === Number(rating)
                );

                return entry ? (entry.RatingProportion * 100).toFixed(0) : 0;
              })
            }))
          };
        },
        options: {
          scales: {
            x: {
              title: {
                display: true,
                text: 'Year'
              }
            },
            y: {
              title: {
                display: true,
                text: 'Rating Proportion (%)'
              },
              // type: 'logarithmic',
              ticks: {
                min: 1,
                suggestedMax: 100,
                callback: function(value) {
                  return value;
                }
              }
            }
          },
          plugins: {
            title: {
              display: true,
              text: 'Average Sentiment Over Time'
            },
            tooltip: {
              callbacks: {
                label: function(tooltipItem) {
                  // Get the rating proportion value and format it as a percentage
                  let rating = tooltipItem.raw;
                  return `Rating: ${rating}%`; // Format as "Rating: 25%" for example
                }
              }
            }
          }
        }
      },

        chart4: {
        type: 'bar',
        title: 'Avg Review Score',
        xTitle: 'Overall Sentiment Score',
        yTitle: 'Review Rating',
        api: '/api/graph/reviewrating_by_overall_sentiment',
        options: {
          scales: {
            y: {
              min: 1,
              max: 5
            }
          }
        },
        mapData: data => ({
          labels: data.map(d => d.SentimentScore),
          datasets: [{
            label: 'Average Review Score',
            data: data.map(d => d.AvgReviewRating),
          }]
        })
      },
      
    },

    sentimentPage: {
      chart1: {
        type: 'bar',
        title: 'Sentiment Trend Over Time',
        xTitle: 'Date',
        yTitle: 'Sentiment',
        api: '/api/graph/reviewrating_by_overall_sentiment',
        mapData: data => ({
          labels: data.map(d => d.OperatorName),
          datasets: [{
            label: 'Sentiment',
            data: data.map(d => d.higherSentiment),
          }]
        })
      }
      // ...
    },

    twoPage: {
      chart1: {
        options: {
          plugins: {
            legend: {
              display: false
            }
          }
        },
        type: 'bar',
        xTitle: 'x',
        yTitle: 'y',
        api: '/api/graph/reviewrating_by_overall_sentiment',
        mapData: data => ({
          labels: data.map(d => d.OperatorName),
          datasets: [{
            label: '',
            data: data.map(d => d.higherSentiment),
          }]
        })
      },
      chart2: {
        options: {
          plugins: {
            legend: {
              display: false
            }
          }
        },
        type: 'bar',
        xTitle: 'x',
        yTitle: 'y',
        api: '/api/graph/reviewrating_by_overall_sentiment',
        mapData: data => ({
          labels: data.map(d => d.OperatorName),
          datasets: [{
            label: '',
            data: data.map(d => d.higherSentiment),
          }]
        })
      }
      // ...
    },

    threePage: {
      chart1: {
        options: {
          plugins: {
            legend: {
              display: false
            }
          }
        },
        type: 'bar',
        xTitle: 'x',
        yTitle: 'y',
        api: '/api/graph/reviewrating_by_overall_sentiment',
        mapData: data => ({
          labels: data.map(d => d.OperatorName),
          datasets: [{
            label: '',
            data: data.map(d => d.higherSentiment),
          }]
        })
      },
      chart2: {
          options: {
          plugins: {
            legend: {
              display: false
            }
          }
        },
        type: 'bar',
        xTitle: 'x',
        yTitle: 'y',
        api: '/api/graph/reviewrating_by_overall_sentiment',
        mapData: data => ({
          labels: data.map(d => d.OperatorName),
          datasets: [{
            label: '',
            data: data.map(d => d.higherSentiment),
          }]
        })
      },
      chart3: {
          options: {
          plugins: {
            legend: {
              display: false
            }
          }
        },
        type: 'bar',
        xTitle: 'x',
        yTitle: 'y',
        api: '/api/graph/reviewrating_by_overall_sentiment',
        mapData: data => ({
          labels: data.map(d => d.OperatorName),
          datasets: [{
            label: '',
            data: data.map(d => d.higherSentiment),
          }]
        })
      }
      // ...
    }
    // ...
  };



  let autoRefreshInterval;
  // experiment with interval, depends on how quick can grab data from SQL. 
  function startAutoRefresh(refreshFn, interval = 5000) {
    if (autoRefreshInterval) clearInterval(autoRefreshInterval);
    refreshFn();
    autoRefreshInterval = setInterval(refreshFn, interval);
  }

  function stopAutoRefresh() {
    if (autoRefreshInterval) clearInterval(autoRefreshInterval);
  }


  function destroyAllCharts() {
    for (const key in window) {
      if (window[key] instanceof Chart) {
        window[key].destroy();
        delete window[key];
      }
    }
  }

  let currentPageKey = null;
  let currentPageVisibleCharts;
  async function loadChartsForPage(pageKey, forceChartReload = false) {
    const pageCharts = pageChartConfigs[pageKey];
    if (!pageCharts) return;
  
    const isPageChanged = currentPageKey !== pageKey;

    // if click on same page selector don't do anything.
    if (!isPageChanged && forceChartReload) return;

    // if changing page, destroy previous page's charts.
    // Also, shrink the previous page's charts, update grid layout, then unshrink the new pages' charts.
    if (isPageChanged || forceChartReload) {
      // collapse any expanded cards.
      collapseExpandedCardClone()

      destroyAllCharts();
      currentPageKey = pageKey;

      const dashboardNavButton = document.getElementById(`${currentPageKey}-btn`);
      activateButtonPersistence(dashboardNavButton)

      currentPageVisibleCharts = Array.from(document.querySelectorAll('.dashboard-graph .chart'))
      .map(chart => chart.closest('.info-card'))
      .filter(card => !card.classList.contains('shrunk'));

      currentPageVisibleCharts.forEach(card => card.classList.add('shrunk'));

      // Cards that will be shown next (needed so can update grid layout before showing cards)
      const pageChartIDs = Object.keys(pageCharts);
      const futureVisibleCards = pageChartIDs.map(id => {
        const chart = document.getElementById(id);
        return chart?.closest('.info-card');
      }).filter(Boolean);



      // match timing with shrunk class transition timing.
      setTimeout(() => {
        updateGridLayout(futureVisibleCards);
      }, 300);
      currentPageVisibleCharts = futureVisibleCards

      document.querySelectorAll('.graph-expand-icon').forEach(svg => svg.remove());

      setTimeout(() => {
        if (currentPageVisibleCharts.length > 1) {
          currentPageVisibleCharts.forEach(card => {
            if (!card.querySelector('.graph-expand-icon')) {
              const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
              svg.setAttribute("class", "graph-expand-icon");
              svg.setAttribute("viewBox", "0 0 24 24");
              svg.setAttribute("width", "24");
              svg.setAttribute("height", "24");
              svg.innerHTML = `<path fill="${getCSSVar('accent-color')}" d="M12 5.83 15.17 9l1.41-1.41L12 3 7.41 7.59 8.83 9zm0 12.34L8.83 15l-1.41 1.41L12 21l4.59-4.59L15.17 15z"/>`;
              svg.style.transform = 'rotate(45deg)'

              Object.assign(svg.style, {
                position: 'absolute',
                top: '8px',
                right: '8px',
                cursor: 'pointer'
              });
        
              card.style.position = 'relative';
              card.appendChild(svg);

              svg.addEventListener('click', () => {
                expandCard(card);
              });
            }
          });
        }
      }, 300);


    }
    

    const chartPromises = Object.entries(pageCharts).map(async ([chartId, config]) => {
      const res = await fetch(config.api);
      const data = await res.json();
      const mapped = config.mapData(data);
      if (!mapped?.labels || !mapped?.datasets) return;
  
      const chart = window[chartId];
  
      if (chart && !forceChartReload) {
        chart.data.labels = mapped.labels;
        mapped.datasets.forEach((ds, i) => {
          chart.data.datasets[i] ? chart.data.datasets[i].data = ds.data : chart.data.datasets[i] = ds;
        });
        chart.update();
      } else {
        renderChartById(chartId, {
          type: config.type,
          data: mapped,
          xTitle: config.xTitle,
          yTitle: config.yTitle,
          options: {
            onHover: function (event, chartElement) {
              const point = chartElement[0];
              event.native.target.style.cursor = point ? 'pointer' : 'default';
            },
            plugins: {
              title: {
                display: true,
                text: config.title
              }
            },
            ...config.options
          }
        });
      }
    });
    
    await Promise.all(chartPromises);

    // Show the cards now that all charts ready
    setTimeout(() => {
      currentPageVisibleCharts.forEach(card => {card.classList.remove('shrunk');});
    }, 0);
    
  }

  // initialise page
  loadChartsForPage('overviewPage', forceChartReload = true)

  // for during processing. - make sure have an auto refresher for the info stats too (it's ready in cycleOperators just need logic for processing = true)
  // startAutoRefresh(() => loadChartsForPage(currentPageKey));
  // stopAutoRefresh()


});

