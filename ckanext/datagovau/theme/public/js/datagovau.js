
function windowOnLoad () {
  addAltToAvatar();
  addTextToI();
  addTitleToSearch();
  insertRequiredNoteBeforeForm();
  textAfterDropdown();
  correctNums();
  navigationInH3();
  viewErrorHide();
}

function viewErrorHide() {
  $('.data-viewer-error [data-toggle="collapse"]').attr('data-toggle','').on('click', function(e){
    $('#data-view-error').toggleClass('collapse');
  })
}

function addAltToAvatar () {
  var name = $('span.username').text();
  var user = $('img.gravatar');
  _updateAttribute(user, 'alt', (name || 'User' ) + ' profile image')
}

function addTitleToSearch () {
  var search = $('input.search');
  _updateAttribute(search, 'title', 'Search:')
  
  search = $('input#field-sitewide-search');
  _updateAttribute(search, 'title', 'Sitewidth search:')
}

function addTextToI () {
  _addReaderTextToButtons('i.icon-remove', 'Remove item');
  _addReaderTextToButtons('form.site-search  i.icon-search', 'Search');
}

function insertRequiredNoteBeforeForm () {
  $('.control-group').first().before($('.control-required-message'));
}

function textAfterDropdown () {
  var icon = $('.resource-item .dropdown-toggle i');
  icon.after($('<span>').addClass('visually-hidden').text(' show below'))
  $('.resource-item .dropdown').on('click', _onDropClick)
}

function correctNums() {
  _repTag('dl','p');
  _repTag('dt','span');
  _repTag('dd','span');
}

function navigationInH3() {
  $('.nav-tabs>li>a').each(function() {
    if (this.innerText && this.innerHTML){
      this.innerHTML = '<h3 class="nav-styled">' + this.innerText + '</h3>'
    } 
    else{
      $(this).html( $('<h3 class="nav-styled">').html( $(this).html() ) )
    }
  })
}


function _updateAttribute (element, attr, value) {
  element.attr(attr, element.attr(attr) || value)
}

function _addReaderTextToButtons(selector, text){
  var self = $(selector);
  $(selector).html('<img src="" class="visually-hidden" alt="'+text+'"/>');
  if ( self.next().is('span') ) return;
  self.after($('<span>').addClass('visually-hidden').text(text));
}

function _onDropClick (event) {
  var self = $(this);
  var target = $(event.target).find('.visually-hidden');
  if (self.hasClass('open')){
    if (target.innerText) target.innerText = ' show below'
    else target.text(' show below');
  } else {
    if (target.innerText) target.innerText = ' hide below'
    else target.text(' hide below');
  }
}

function _repTag(old, updated) {
  $(old).each(function () {
    $(this).replaceWith( $('<'+updated+'>').html( $(this).html() ).addClass(old) )
  });
}

window.onload = windowOnLoad()