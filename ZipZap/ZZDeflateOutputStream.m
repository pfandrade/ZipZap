//
//  ZZDeflateOutputStream.m
//  ZipZap
//
//  Created by Glen Low on 9/10/12.
//  Copyright (c) 2012, Pixelglow Software. All rights reserved.
//

#include <zlib.h>

#import "ZZChannelOutput.h"
#import "ZZDeflateOutputStream.h"

static const uInt _flushLength = 1024;


@interface ZZRunLoopSource: NSObject

+ (instancetype)sourceScheduledInRunloop:(NSRunLoop *)runloop mode:(NSRunLoopMode)mode order:(NSUInteger)order handlingBlock:(void(^)(void))block;

@property (strong) NSRunLoop *runLoop;
@property  NSRunLoopMode mode;
@property  NSUInteger order;
@property  (copy) void(^performBlock)(void);

- (void)schedule;
- (void)unschedule;
- (void)signal;

@end

@interface ZZDeflateOutputStream () <NSStreamDelegate>

@end


@implementation ZZDeflateOutputStream
{
	id<ZZChannelOutput> _channelOutput;
	NSUInteger _compressionLevel;
	NSStreamStatus _status;
	NSError* _error;
	uint32_t _crc32;
	z_stream _stream;
	
	id<NSStreamDelegate> _delegate;
	NSMutableArray<ZZRunLoopSource *> *_runLoopSources;
	NSMutableArray<NSNumber *> *_eventQueue;
}

@synthesize crc32 = _crc32;

- (instancetype)initWithChannelOutput:(id<ZZChannelOutput>)channelOutput
					 compressionLevel:(NSUInteger)compressionLevel
{
	if ((self = [super init]))
	{
		_channelOutput = channelOutput;
		_compressionLevel = compressionLevel;
		
		_status = NSStreamStatusNotOpen;
		_error = nil;
		_crc32 = 0;
		_stream.zalloc = Z_NULL;
		_stream.zfree = Z_NULL;
		_stream.opaque = Z_NULL;
		_stream.next_in = Z_NULL;
		_stream.avail_in = 0;
		
		_runLoopSources = [[NSMutableArray alloc] init];
	}
	return self;
}

- (void)dealloc
{
	[_runLoopSources makeObjectsPerformSelector:@selector(unschedule)];
}

- (uint32_t)compressedSize
{
	return (uint32_t)_stream.total_out;
}

- (uint32_t)uncompressedSize
{
	return (uint32_t)_stream.total_in;
}

- (NSStreamStatus)streamStatus
{
	return _status;
}

- (NSError*)streamError
{
	return _error;
}

- (void)open
{
	deflateInit2(&_stream,
				 _compressionLevel,
				 Z_DEFLATED,
				 -15,
				 8,
				 Z_DEFAULT_STRATEGY);
	_status = NSStreamStatusOpen;
	[self enqueueEvent:NSStreamEventOpenCompleted];
	[self enqueueEvent:NSStreamEventHasSpaceAvailable];
}

- (void)close
{
	uint8_t flushBuffer[_flushLength];
	_stream.next_in = Z_NULL;
	_stream.avail_in = 0;
	
	// flush out all remaining deflated bytes, a bufferfull at a time
	BOOL flushing = YES;
	while (flushing)
	{
		_stream.next_out = flushBuffer;
		_stream.avail_out = _flushLength;
		
		flushing = deflate(&_stream, Z_FINISH) == Z_OK;
				
		if (_stream.avail_out < _flushLength)
		{
			NSError* __autoreleasing flushError;
			if (![_channelOutput writeData:[NSData dataWithBytesNoCopy:flushBuffer
																length:_flushLength - _stream.avail_out
														  freeWhenDone:NO]
									 error:&flushError])
			{
				_status = NSStreamStatusError;
				_error = flushError;
			}
		}
	}
	
	deflateEnd(&_stream);
	_status = NSStreamStatusClosed;
}

- (NSInteger)write:(const uint8_t*)buffer maxLength:(NSUInteger)length
{
	// allocate an output buffer large enough to fit deflated output
	// NOTE: we ensure that we can deflate at least one byte, since write:maxLength: need not deflate all bytes
	uLong maxLength = deflateBound(&_stream, length);
	NSMutableData* outputBuffer = [[NSMutableData alloc] initWithLength:maxLength];
	
	// deflate buffer to output buffer
	_stream.next_in = (Bytef*)buffer;
	_stream.avail_in = (uInt)length;
	_stream.next_out = (Bytef*)outputBuffer.mutableBytes;
	_stream.avail_out = (uInt)maxLength;
	deflate(&_stream, Z_NO_FLUSH);
	
	// write out deflated output if any
	outputBuffer.length = maxLength - _stream.avail_out;
	if (outputBuffer.length > 0)
	{
		NSError* __autoreleasing writeError;
		if (![_channelOutput writeData:outputBuffer
								 error:&writeError])
		{
			_status = NSStreamStatusError;
			_error = writeError;
			return -1;
		}
	}

	// accumulate checksum only on bytes that were deflated
	NSUInteger bytesWritten = length - _stream.avail_in;
	_crc32 = (uint32_t)crc32(_crc32, buffer, (uInt)bytesWritten);
	
	[self enqueueEvent:NSStreamEventHasSpaceAvailable];
	return bytesWritten;
}

- (BOOL)hasSpaceAvailable
{
	return YES;
}

- (id<NSStreamDelegate>)delegate
{
	return _delegate;
}

- (void)setDelegate:(id<NSStreamDelegate>)delegate
{
	_delegate = delegate;
	if (_delegate == nil) {
		_delegate = self;
	}
}

#pragma mark - Runloop handling

- (void)enqueueEvent:(NSStreamEvent)event
{
	[_eventQueue addObject:@(event)];
    [_runLoopSources makeObjectsPerformSelector:@selector(signal)];
}

- (NSStreamEvent)dequeueEvent
{
	NSNumber *e = [_eventQueue firstObject];
	if (e == nil) {
		return NSStreamEventNone;
	} else {
		[_eventQueue removeObjectAtIndex:0];
	}

    if ([_eventQueue count] > 0) {
        [_runLoopSources makeObjectsPerformSelector:@selector(signal)];
    }
	return [e unsignedIntegerValue];
}

- (void)dequeueAndNotify
{
	NSStreamEvent event = [self dequeueEvent];
	if (event != NSStreamEventNone) {
		[self.delegate stream:self handleEvent:event];
	}
}

- (void)scheduleInRunLoop:(NSRunLoop *)aRunLoop forMode:(NSRunLoopMode)mode
{
	__weak ZZDeflateOutputStream *wSelf = self;
	ZZRunLoopSource *source = [ZZRunLoopSource sourceScheduledInRunloop:aRunLoop mode:mode order:0 handlingBlock:^{
		[wSelf dequeueAndNotify];
	}];
	
	[_runLoopSources addObject:source];
	if (_eventQueue == nil) {
		_eventQueue = [[NSMutableArray alloc] init];
	}
}

- (void)removeFromRunLoop:(NSRunLoop *)aRunLoop forMode:(NSRunLoopMode)mode
{
	ZZRunLoopSource *source = nil;
	for (ZZRunLoopSource *aSource in _runLoopSources) {
		if ([aSource.runLoop isEqual:aRunLoop] && [aSource.mode isEqualToString:mode]) {
			source = aSource;
			break;
		}
	}
	[_runLoopSources removeObject:source];
	[source unschedule];
	
	if ([_runLoopSources count] == 0) {
		_eventQueue = nil;
	}
}



@end


static void runLoopSourceCallback(void *info);

@implementation ZZRunLoopSource {
	CFRunLoopSourceRef _sourceRef;
}

+ (instancetype)sourceScheduledInRunloop:(NSRunLoop *)runloop mode:(NSRunLoopMode)mode order:(NSUInteger)order handlingBlock:(void (^)(void))block
{
	ZZRunLoopSource *source = [[ZZRunLoopSource alloc] init];
	source.runLoop = runloop;
	source.order = order;
	source.mode = mode;
	source.performBlock = block;
	[source schedule];
	return source;
}

- (void)dealloc
{
	[self unschedule];
}

- (CFRunLoopSourceRef)getSourceRef
{
	if (_sourceRef == NULL) {
		
	}
	return _sourceRef;
}

- (void)schedule
{
	if(_sourceRef == NULL) {
		CFRunLoopSourceContext context;
        bzero(&context, sizeof(CFRunLoopSourceContext));
		context.info = (__bridge void *)(self);
		context.perform = runLoopSourceCallback;
		_sourceRef = CFRunLoopSourceCreate(NULL, self.order, &context);
		CFRunLoopAddSource([self.runLoop getCFRunLoop], _sourceRef, (CFRunLoopMode)self.mode);
	}
}

- (void)unschedule
{
	if (_sourceRef != NULL){
		CFRunLoopRemoveSource([self.runLoop getCFRunLoop], _sourceRef, (CFRunLoopMode)self.mode);
		CFRelease(_sourceRef);
		_sourceRef = NULL;
	}
}

- (void)signal
{
	if (_sourceRef) {
		CFRunLoopSourceSignal(_sourceRef);
	}
}

@end

static void runLoopSourceCallback(void *info){
	ZZRunLoopSource *source = (__bridge ZZRunLoopSource *)info;
	source.performBlock();
}
