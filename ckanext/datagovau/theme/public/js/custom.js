window.onload = windowOnLoad()



function windowOnLoad () {
  addAltToAvatar();
  addTextToI();
  addTitleToSearch();
  fixPager();
  insertRequiredNoteBeforeForm();
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

function fixPager () {
  $('div.pagination li.disabled').text('<span>...</span>');
  var first = $('div.pagination a').first()
  try{
    first.text(a.text().replace('«', 'Next page'))
  }
  catch(e){
    return
  }
  var last = $('div.pagination a').last()
  last.text(a.text().replace('»', 'Previous page'))
}


// u'«', symbol_next=u'»',

function _updateAttribute (element, attr, value) {
  element.attr(attr, element.attr(attr) || value)
}

function _addReaderTextToButtons(selector, text){
  var self = $(selector);
  if ( self.next().is('span') ) return;
  self.after($('<span>').addClass('visually-hidden').text(text))
}

function insertRequiredNoteBeforeForm () {
  $('.control-group').first().before($('.control-required-message'));
}